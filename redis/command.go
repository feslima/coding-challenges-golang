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
	PING     = "PING"
	ECHO     = "ECHO"
	SET      = "SET"
	GET      = "GET"
	CONFIG   = "CONFIG"
	EXPIRE   = "EXPIRE"
	EXPIREAT = "EXPIREAT"
	EXISTS   = "EXISTS"
	DEL      = "DEL"
	INCR     = "INCR"
	DECR     = "DECR"
	RPUSH    = "RPUSH"
	LPUSH    = "LPUSH"
)

var cmdParseTable = map[string]Command{
	"ping":     PING,
	"echo":     ECHO,
	"set":      SET,
	"get":      GET,
	"config":   CONFIG,
	"expire":   EXPIRE,
	"expireat": EXPIREAT,
	"exists":   EXISTS,
	"del":      DEL,
	"incr":     INCR,
	"decr":     DECR,
	"rpush":    RPUSH,
	"lpush":    LPUSH,
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

	case EXPIREAT:
		return processExpireAt(c.args, a)

	case EXISTS:
		return processExists(c.args, a)

	case DEL:
		return processDelete(c.args, a)

	case INCR:
		return processIncrement(c.args, a)

	case DECR:
		return processDecrement(c.args, a)

	case RPUSH:
		return processRPush(c.args, a)

	case LPUSH:
		return processLPush(c.args, a)
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

	var expiry *ExpiryDuration
	if nArgs > 2 {
		resolutionType := strings.ToUpper(args[2])
		if resolutionType != "EX" && resolutionType != "PX" {
			return "", errors.New("invalid resolution type")
		}

		var resolution time.Duration
		if resolutionType == "EX" {
			resolution = time.Second
		} else {
			resolution = time.Millisecond
		}

		delta, err := strconv.ParseInt(args[3], 10, 0)
		if err != nil {
			return "", err
		}
		expiry = &ExpiryDuration{magnitude: delta, resolution: resolution}
	} else {
		expiry = nil
	}

	app.state.keyspace.SetKey(key, value, expiry)
	app.state.CountModification()

	return OK_SIMPLE_STRING, nil
}

func processGet(args []string, app *Application) (string, error) {
	if len(args) != 1 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	k := app.state.keyspace.Get(key)
	if !k.IsValid() || !k.IsString() {
		return NIL_BULK_STRING, nil
	}

	return SerializeBulkString(*k.str), nil
}

func processConfig(args []string, app *Application) (string, error) {
	if len(args) < 2 {
		return "", wrongNumOfArgsErr
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
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	rawDelta := args[1]

	delta, err := strconv.ParseInt(rawDelta, 10, 0)
	if err != nil {
		msg := fmt.Sprintf("could not parse '%s' to integer", rawDelta)
		return SerializeSimpleError(msg), nil
	}

	ok := app.state.keyspace.Expire(key, delta)
	if !ok {
		return SerializeInteger(0), nil
	}
	app.state.CountModification()

	return SerializeInteger(1), nil
}

func processExpireAt(args []string, app *Application) (string, error) {
	if len(args) != 2 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	rawStamp := args[1]

	stamp, err := strconv.ParseInt(rawStamp, 10, 0)
	if err != nil {
		msg := fmt.Sprintf("could not parse '%s' to integer", rawStamp)
		return SerializeSimpleError(msg), nil
	}

	deadline := time.Unix(stamp, 0)
	ok := app.state.keyspace.ExpireAt(key, deadline)
	if !ok {
		return SerializeInteger(0), nil
	}
	app.state.CountModification()

	return SerializeInteger(1), nil
}

func processExists(args []string, app *Application) (string, error) {
	if len(args) < 1 {
		return "", wrongNumOfArgsErr
	}

	keyCount := app.state.keyspace.BulkExists(args)

	finalCount := 0
	for _, c := range keyCount {
		if c > 0 {
			finalCount += c
		}
	}
	return SerializeInteger(finalCount), nil
}

func processDelete(args []string, app *Application) (string, error) {
	if len(args) < 1 {
		return "", wrongNumOfArgsErr
	}

	keyCount := app.state.keyspace.BulkDelete(args)
	app.state.CountModification()

	finalCount := 0
	for _, c := range keyCount {
		if c > 0 {
			finalCount += c
		}
	}
	return SerializeInteger(finalCount), nil
}

func processIncrement(args []string, app *Application) (string, error) {
	if len(args) != 1 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	value, err := app.state.keyspace.IncrementBy(key, 1)
	if err != nil {
		return SerializeSimpleError(err.Error()), nil
	}

	app.state.CountModification()
	return SerializeInteger(value), nil
}

func processDecrement(args []string, app *Application) (string, error) {
	if len(args) != 1 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	value, err := app.state.keyspace.IncrementBy(key, -1)
	if err != nil {
		return SerializeSimpleError(err.Error()), nil
	}

	app.state.CountModification()
	return SerializeInteger(value), nil
}

func processRPush(args []string, app *Application) (string, error) {
	if len(args) < 1 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	values := args[1:]

	length, err := app.state.keyspace.PushToTail(key, values)
	if err != nil {
		return SerializeSimpleError(err.Error()), nil
	}

	app.state.CountModification()
	return SerializeInteger(length), nil
}

func processLPush(args []string, app *Application) (string, error) {
	if len(args) < 1 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	values := args[1:]

	length, err := app.state.keyspace.PushToHead(key, values)
	if err != nil {
		return SerializeSimpleError(err.Error()), nil
	}

	app.state.CountModification()
	return SerializeInteger(length), nil
}
