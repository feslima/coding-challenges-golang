package redis

import (
	"errors"
	"fmt"
	"strings"
)

type Command string

const (
	PING   = "PING"
	ECHO   = "ECHO"
	SET    = "SET"
	GET    = "GET"
	CONFIG = "CONFIG"
)

var cmdParseTable = map[string]Command{
	"ping":   PING,
	"echo":   ECHO,
	"set":    SET,
	"get":    GET,
	"config": CONFIG,
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
	}
}

func processEcho(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("wrong number of arguments.")
	}

	return SerializeBulkString(args[0]), nil
}

func processSet(args []string, app *Application) (string, error) {
	if len(args) != 2 {
		return "", errors.New("wrong number of arguments.")
	}

	key := args[0]
	value := args[1]

	state := app.state.stringMap
	state[key] = value

	return SerializeSimpleString("OK"), nil
}

func processGet(args []string, app *Application) (string, error) {
	if len(args) != 1 {
		return "", errors.New("wrong number of arguments.")
	}

	value, ok := app.state.stringMap[args[0]]
	if !ok {
		return NIL_BULK_STRING, nil
	}

	return SerializeBulkString(value), nil
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
