package redis

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Command string

type CommandResult struct {
	message []byte
	targets []net.Conn
}

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

type Cmd struct {
	app       *Application
	processed []string
	cmd       Command
	args      []string
	sender    net.Conn
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

func (c *Cmd) Process() (*CommandResult, error) {
	err := c.Parse()
	targets := []net.Conn{c.sender}
	if err != nil {
		return &CommandResult{message: []byte(""), targets: targets}, err
	}

	var r string

	switch c.cmd {
	default:
		r = ""
		err = errors.New("invalid command")

	case PING:
		r = "+PONG\r\n"
		err = nil

	case ECHO:
		r, err = processEcho(c.args)

	case SET:
		r, err = processSet(c.args, c.app)

	case GET:
		r, err = processGet(c.args, c.app)

	case CONFIG:
		r, err = processConfig(c.args, c.app)

	case EXPIRE:
		r, err = processExpire(c.args, c.app)

	case EXPIREAT:
		r, err = processExpireAt(c.args, c.app)

	case EXISTS:
		r, err = processExists(c.args, c.app)

	case DEL:
		r, err = processDelete(c.args, c.app)

	case INCR:
		r, err = processIncrement(c.args, c.app)

	case DECR:
		r, err = processDecrement(c.args, c.app)

	case RPUSH:
		r, err = processRPush(c.args, c.app)

	case LPUSH:
		r, err = processLPush(c.args, c.app)
	}

	return &CommandResult{message: []byte(r), targets: targets}, err
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

		// this is supposed to be a slice of strings, however go forces
		// us to use a slice of interface to allow array serialization
		configs := make([]interface{}, len(params))

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

	return SerializeInteger(length), nil
}
