package redis

import (
	"errors"
	"strconv"
)

type RESPType byte

const (
	Array      RESPType = '*'
	BulkString RESPType = '$'
)

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
		return errors.New("data does not match length.")
	}

	dataChunk := string(raw[dataStartIndex : len(raw)-2])
	c.parsed = []string{dataChunk}
	return nil
}

func DecodeMessage(rawMessage []byte) (*Cmd, error) {
	if len(rawMessage) == 0 {
		return nil, errors.New("Got an empty message")
	}
	firstByte := rawMessage[0]

	cmd := Cmd{parsed: nil}

	switch firstByte {
	case byte(BulkString):
		err := cmd.decodeBulkString(rawMessage[1:])
		return &cmd, err
	}

	return nil, nil
}
