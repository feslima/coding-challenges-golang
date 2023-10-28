package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type RESPType byte

const (
	Array      RESPType = '*'
	BulkString RESPType = '$'
)

const NIL_BULK_STRING = "$-1\r\n"

func getFirstCRIndex(raw []byte) int64 {
	crIndex := int64(0)
	for i, c := range raw {
		if c == '\r' {
			crIndex = int64(i)
			break
		}
	}

	return crIndex
}

type Cmd struct {
	processed []string
	cmd       Command
	args      []string
}

func decodeBulkString(raw []byte) ([]string, error) {
	rawLength := []rune{}
	dataStartIndex := int64(0)
	for i, c := range raw {
		if c == '\r' {
			dataStartIndex = int64(i) + 2
			break
		}
		rawLength = append(rawLength, rune(c))
	}

	rawString := string(rawLength)
	if rawString[0] == '-' && rawString != "-1" {
		return nil, errors.New("invalid null string")
	}

	length, err := strconv.ParseInt(rawString, 10, 0)
	if err != nil {
		return nil, err
	}

	if length == -1 {
		return nil, err
	}

	if length == 0 {
		return []string{""}, nil
	}

	if raw[len(raw)-2] != raw[dataStartIndex+length] {
		return nil, errors.New("data does not match length")
	}

	dataChunk := string(raw[dataStartIndex : len(raw)-2])
	return []string{dataChunk}, nil
}

func decodeArray(raw []byte) ([]string, error) {
	crIndex := getFirstCRIndex(raw)

	s := string(raw)
	numOfElements, err := strconv.ParseUint(string(s[:crIndex]), 10, 0)
	if err != nil {
		return nil, errors.New("failed to parse number of elements to unsigned int")
	}

	parsed := make([]string, 0)
	if numOfElements == 0 {
		return parsed, nil
	}

	split := strings.Split(s[crIndex+2:], "\r\n")
	if split[len(split)-1] == "" {
		split = split[:len(split)-1]
	}

	for i := 0; i < len(split); i += 2 {
		rawLength := split[i][1:]
		length, err := strconv.ParseInt(rawLength, 10, 0)
		if err != nil {
			return nil, err
		}

		data := split[i+1]
		if int64(len(data)) != length {
			return nil, fmt.Errorf("length and data mismatch. received length: %d. data length: %d", length, len(data))
		}

		parsed = append(parsed, data)
	}
	return parsed, nil
}

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
		return ProcessEcho(c.args)

	case SET:
		return ProcessSet(c.args, a)

	case GET:
		return ProcessGet(c.args, a)

	case CONFIG:
		return ProcessConfig(c.args, a)
	}
}

func ProcessEcho(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("wrong number of arguments.")
	}

	return SerializeBulkString(args[0]), nil
}

func ProcessSet(args []string, app *Application) (string, error) {
	if len(args) != 2 {
		return "", errors.New("wrong number of arguments.")
	}

	key := args[0]
	value := args[1]

	state := app.state.stringMap
	state[key] = value

	return SerializeSimpleString("OK"), nil
}

func ProcessGet(args []string, app *Application) (string, error) {
	if len(args) != 1 {
		return "", errors.New("wrong number of arguments.")
	}

	value, ok := app.state.stringMap[args[0]]
	if !ok {
		return NIL_BULK_STRING, nil
	}

	return SerializeBulkString(value), nil
}

func ProcessConfig(args []string, app *Application) (string, error) {
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

func DecodeMessage(rawMessage []byte) (*Cmd, error) {
	if len(rawMessage) == 0 {
		return nil, errors.New("Got an empty message")
	}
	firstByte := rawMessage[0]
	remaining := rawMessage[1:]

	cmd := Cmd{processed: nil}

	var err error
	switch firstByte {
	case byte(BulkString):
		parsed, err := decodeBulkString(remaining)
		if err != nil {
			return nil, err
		}
		cmd.processed = parsed
	case byte(Array):
		parsed, err := decodeArray(remaining)
		if err != nil {
			return nil, err
		}
		cmd.processed = parsed
	default:
		err = errors.New("invalid first byte")
	}

	return &cmd, err
}

func SerializeBulkString(data string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
}

func SerializeSimpleString(data string) string {
	return fmt.Sprintf("+%s\r\n", data)
}

func SerializeSimpleError(data string) string {
	return fmt.Sprintf("-%s\r\n", data)
}

func SerializeArray(data []string) string {
	length := int64(len(data))
	result := fmt.Sprintf("*%d\r\n", length)

	if length == 0 {
		return result
	}

	for _, v := range data {
		string := SerializeBulkString(v)
		result += string
	}

	return result
}
