package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Command string

const (
	PING   = "PING"
	ECHO   = "ECHO"
	SET    = "SET"
	GET    = "GET"
	CONFIG = "CONFIG"
	EXPIRE = "EXPIRE"
	EXISTS = "EXISTS"
)

var cmdParseTable = map[string]Command{
	"ping":   PING,
	"echo":   ECHO,
	"set":    SET,
	"get":    GET,
	"config": CONFIG,
	"expire": EXPIRE,
	"exists": EXISTS,
}

func (c *Cmd) Parse() error {
	lower := strings.ToLower(c.processed[0])
	cmd, ok := cmdParseTable[lower]
	if !ok {
		return fmt.Errorf("invalid command: '%s'", lower)
	}

	c.cmd = cmd
	c.args = c.processed[1:]

	return nil
}

func (c *Cmd) Process(a *Application) (string, error) {
	err := c.Parse()
	if err != nil {
		return "", err
	}

	switch c.cmd {
	default:
		return "", errors.New("invalid command")

	case PING:
		return "+PONG\r\n", nil

	case ECHO:
		return processEcho(c.args)

	case SET:
		return processSet(c.args, a)

	case GET:
		return processGet(c.args, a)

	case CONFIG:
		return processConfig(c.args, a)

	case EXPIRE:
		return processExpire(c.args, a)

	case EXISTS:
		return processExists(c.args, a)
	}
}

var wrongNumOfArgsErr = errors.New("wrong number of arguments.")

func processEcho(args []string) (string, error) {
	if len(args) != 1 {
		return "", wrongNumOfArgsErr
	}

	return SerializeBulkString(args[0]), nil
}

func processSet(args []string, app *Application) (string, error) {
	nArgs := len(args)
	if nArgs < 2 {
		return "", wrongNumOfArgsErr
	}

	if nArgs > 2 && nArgs != 4 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	value := args[1]

	var expiry *time.Time
	if nArgs > 2 {
		resolution := strings.ToUpper(args[2])
		if resolution != "EX" && resolution != "PX" {
			return "", errors.New("invalid resolution type")
		}

		var resolutionType time.Duration
		if resolution == "EX" {
			resolutionType = time.Second
		} else {
			resolutionType = time.Millisecond
		}

		delta, err := strconv.ParseInt(args[3], 10, 0)
		if err != nil {
			return "", err
		}
		final := app.clock.Now().Add(time.Duration(delta) * resolutionType)
		expiry = &final
	} else {
		expiry = nil
	}

	app.state.mutex.Lock()
	state := app.state.stringMap
	state[key] = StringValue{value: value, expires: expiry}
	app.state.mutex.Unlock()

	return OK_SIMPLE_STRING, nil
}

func processGet(args []string, app *Application) (string, error) {
	if len(args) != 1 {
		return "", errors.New("wrong number of arguments.")
	}

	key := args[0]
	app.state.mutex.RLock()
	sv, ok := app.state.stringMap[key]
	app.state.mutex.RUnlock()
	if !ok {
		return NIL_BULK_STRING, nil
	}

	if sv.expires != nil && app.clock.Now().After(*sv.expires) {
		app.state.mutex.Lock()
		delete(app.state.stringMap, key)
		app.state.mutex.Unlock()
		return NIL_BULK_STRING, nil
	}

	return SerializeBulkString(sv.value), nil
}

func processConfig(args []string, app *Application) (string, error) {
	if len(args) < 2 {
		return "", errors.New("wrong number of arguments.")
	}

	cmd := strings.ToUpper(args[0])
	switch cmd {
	default:
		return SerializeSimpleError(fmt.Sprintf("invalid cmd '%s'", cmd)), nil
	case "GET":
		params := args[1:]
		configs := []string{}

		for _, p := range params {
			p = strings.ToLower(p)
			if _, ok := configMap[p]; !ok {
				return SerializeSimpleError(fmt.Sprintf("invalid parameter '%s'", p)), nil
			}

			switch p {
			case "appendonly":
				configs = append(configs, p)
				configs = append(configs, app.config.appendonly)

			case "save":
				configs = append(configs, p)
				configs = append(configs, app.config.save)
			}

		}

		return SerializeArray(configs), nil

	}
}

func processExpire(args []string, app *Application) (string, error) {
	if len(args) != 2 {
		return "", errors.New("wrong number of arguments.")
	}

	key := args[0]
	app.state.mutex.RLock()
	sv, ok := app.state.stringMap[key]
	app.state.mutex.RUnlock()
	if !ok {
		return SerializeInteger(0), nil
	}

	value := args[1]
	delta, err := strconv.ParseInt(value, 10, 0)
	if err != nil {
		return "", err
	}

	var final time.Time
	if sv.expires == nil {
		final = app.clock.Now().Add(time.Duration(delta) * time.Second)
	} else {
		// update by adding time to key expiry
		final = sv.expires.Add(time.Duration(delta) * time.Second)
	}

	sv.expires = &final

	app.state.mutex.Lock()
	app.state.stringMap[key] = sv
	app.state.mutex.Unlock()

	return SerializeInteger(1), nil
}

func processExists(args []string, app *Application) (string, error) {
	if len(args) < 1 {
		return "", errors.New("wrong number of arguments.")
	}

	keyCount := map[string]int{}
	app.state.mutex.RLock()
	for _, key := range args {
		_, ok := app.state.stringMap[key]
		_, kcOk := keyCount[key]
		if ok {
			if kcOk {
				keyCount[key] += 1
			} else {
				keyCount[key] = 1
			}
		} else {
			keyCount[key] = 0
		}
	}
	app.state.mutex.RUnlock()

	finalCount := 0
	for _, c := range keyCount {
		if c > 0 {
			finalCount += c
		}
	}
	return SerializeInteger(finalCount), nil
}
