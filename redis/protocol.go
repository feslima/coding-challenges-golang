package redis

import (
	"errors"
	"strconv"
	"strings"
)

type RESPType byte

const (
	Array      RESPType = '*'
	BulkString RESPType = '$'
)

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
	parsed []string
}

func (c *Cmd) decodeBulkString(raw []byte) error {
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
		return errors.New("invalid null string")
	}

	length, err := strconv.ParseInt(rawString, 10, 0)
	if err != nil {
		return err
	}

	if length == -1 {
		c.parsed = nil
		return nil
	}

	if length == 0 {
		c.parsed = []string{""}
		return nil
	}

	if raw[len(raw)-2] != raw[dataStartIndex+length] {
		return errors.New("data does not match length")
	}

	dataChunk := string(raw[dataStartIndex : len(raw)-2])
	c.parsed = []string{dataChunk}
	return nil
}

func (c *Cmd) decodeArray(raw []byte) error {
	crIndex := getFirstCRIndex(raw)

	s := string(raw)
	numOfElements, err := strconv.ParseUint(string(s[:crIndex]), 10, 0)
	if err != nil {
		return errors.New("failed to parse number of elements to unsigned int")
	}

	c.parsed = make([]string, 0)
	if numOfElements == 0 {
		return nil
	}

	split := strings.Split(s[crIndex+2:], "\r\n")
	if split[len(split)-1] == "" {
		split = split[:len(split)-1]
	}

	for i := 0; i < len(split); i += 2 {
		rawLength := split[i][1:]
		length, err := strconv.ParseInt(rawLength, 10, 0)
		if err != nil {
			c.parsed = nil
			return err
		}

		data := split[i+1]
		if int64(len(data)) != length {
			c.parsed = nil
			return errors.New("length and data mismatch")
		}

		c.parsed = append(c.parsed, data)
	}
	return nil
}

func DecodeMessage(rawMessage []byte) (*Cmd, error) {
	if len(rawMessage) == 0 {
		return nil, errors.New("Got an empty message")
	}
	firstByte := rawMessage[0]
	remaining := rawMessage[1:]

	cmd := Cmd{parsed: nil}

	var err error
	switch firstByte {
	case byte(BulkString):
		err = cmd.decodeBulkString(remaining)
	case byte(Array):
		err = cmd.decodeArray(remaining)
	default:
		err = errors.New("invalid command")
	}

	return &cmd, err
}
