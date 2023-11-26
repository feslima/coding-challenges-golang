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
	PING      = "PING"
	ECHO      = "ECHO"
	SET       = "SET"
	GET       = "GET"
	CONFIG    = "CONFIG"
	EXPIRE    = "EXPIRE"
	EXPIREAT  = "EXPIREAT"
	EXISTS    = "EXISTS"
	DEL       = "DEL"
	INCR      = "INCR"
	DECR      = "DECR"
	RPUSH     = "RPUSH"
	LPUSH     = "LPUSH"
	SUBSCRIBE = "SUBSCRIBE"
	PUBLISH   = "PUBLISH"
	ZADD      = "ZADD"
	ZRANGE    = "ZRANGE"
)

var cmdParseTable = map[string]Command{
	"ping":      PING,
	"echo":      ECHO,
	"set":       SET,
	"get":       GET,
	"config":    CONFIG,
	"expire":    EXPIRE,
	"expireat":  EXPIREAT,
	"exists":    EXISTS,
	"del":       DEL,
	"incr":      INCR,
	"decr":      DECR,
	"rpush":     RPUSH,
	"lpush":     LPUSH,
	"subscribe": SUBSCRIBE,
	"publish":   PUBLISH,
	"zadd":      ZADD,
	"zrange":    ZRANGE,
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

	case SUBSCRIBE:
		r, err = processSubscribe(c.args, c.sender, c.app)

	case PUBLISH:
		r, targets, err = processPublish(c.args, c.sender, c.app)

		// REFACTOR: I find this rather ugly/cumbersome to write a response to publisher connection
		// while still inside the command.
		c.sender.Write([]byte(SerializeInteger(len(targets))))

	case ZADD:
		r, err = processZAdd(c.args, c.app)

	case ZRANGE:
		r, err = processZRange(c.args, c.app)
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

func processSubscribe(args []string, sender net.Conn, app *Application) (string, error) {
	if len(args) < 1 {
		return "", wrongNumOfArgsErr
	}

	client, err := app.GetClient(sender)
	if err != nil {
		return "", err
	}

	response := ""
	for i, cName := range args {
		app.SubscribeConnection(cName, sender)
		client.SubscribeTo(cName)

		arr := make([]interface{}, 0)
		arr = append(arr, "subscribe")
		arr = append(arr, cName)
		arr = append(arr, i+1)

		response += SerializeArray(arr)
	}

	return response, nil
}

func processPublish(args []string, sender net.Conn, app *Application) (string, []net.Conn, error) {
	if len(args) != 2 {
		return "", []net.Conn{}, wrongNumOfArgsErr
	}

	channel := args[0]
	message := args[1]

	targets := app.GetConnectionsPerChannelExcludingConn(channel, sender)
	if len(targets) == 0 {
		app.pubsubChannels[channel] = make(map[string]net.Conn)
	}

	result := make([]interface{}, 0)
	result = append(result, "message")
	result = append(result, channel)
	result = append(result, message)

	response := SerializeArray(result)
	return response, targets, nil
}

func processZAdd(args []string, app *Application) (string, error) {
	if len(args) < 3 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	values := args[1:]

	if len(values)%2 != 0 {
		msg := "<score> <member> values must come in pairs"
		return SerializeSimpleError(msg), nil
	}

	for i := 0; i < len(values); i += 2 {
		rawScore := values[i]
		_, err := strconv.ParseFloat(rawScore, 64)
		if err != nil {
			msg := fmt.Sprintf("could not parse '%s' to float", rawScore)
			return SerializeSimpleError(msg), nil
		}
	}

	length, err := app.state.keyspace.PutInSortedSet(key, values)
	if err != nil {
		return SerializeSimpleError(err.Error()), nil
	}

	return SerializeInteger(length), nil
}

func processZRange(args []string, app *Application) (string, error) {
	if len(args) != 3 {
		return "", wrongNumOfArgsErr
	}

	key := args[0]
	rawStart := args[1]
	rawStop := args[2]

	start, err := strconv.ParseInt(rawStart, 0, 10)
	if err != nil {
		msg := fmt.Sprintf("could not parse '%s' to integer", rawStart)
		return SerializeSimpleError(msg), nil
	}

	stop, err := strconv.ParseInt(rawStop, 0, 10)
	if err != nil {
		msg := fmt.Sprintf("could not parse '%s' to integer", rawStop)
		return SerializeSimpleError(msg), nil
	}

	values, err := app.state.keyspace.GetSortedSetValuesByRange(key, start, stop)
	if err != nil {
		return SerializeSimpleError(err.Error()), nil
	}

	result := make([]interface{}, 0)
	for _, v := range values {
		result = append(result, v)
	}
	response := SerializeArray(result)

	return response, nil
}
