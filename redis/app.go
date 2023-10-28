package redis

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

type ClockTimer interface {
	Now() time.Time
}

type RealClockTimer struct{}

func (c RealClockTimer) Now() time.Time {
	return time.Now()
}

type Application struct {
	state  *ApplicationState
	config *ApplicationConfiguration
}

func NewApplication(config *ApplicationConfiguration, timer ClockTimer) *Application {
	state := ApplicationState{
		clock:     timer,
		stringMap: make(map[string]StringValue),
	}
	return &Application{state: &state, config: config}
}

func (app *Application) ProcessRequest(raw []byte) ([]byte, error) {
	command, err := DecodeMessage(raw)
	if err != nil {
		slog.Error("error decoding message: " + fmt.Sprintf("%v", err))
		return []byte{}, err
	}
	response, err := command.Process(app)
	if err != nil {
		slog.Error("error parsing message: " + fmt.Sprintf("%v", err))
		return []byte{}, err
	}

	return []byte(response), nil
}

type StringValue struct {
	value   string
	expires *time.Time
}

type ApplicationState struct {
	clock     ClockTimer
	stringMap map[string]StringValue
}

var validSaveOptions map[string]bool = map[string]bool{"yes": true, "no": true}

var configMap map[string]bool = map[string]bool{"appendonly": true, "save": true}

type ApplicationConfiguration struct {
	appendonly string
	save       string
}

func NewApplicationConfiguration(appendonly string, save string) (*ApplicationConfiguration, error) {
	ac := ApplicationConfiguration{
		appendonly: appendonly,
		save:       save,
	}

	err := ac.validateAppendOnly()
	if err != nil {
		return nil, err
	}

	err = ac.validateSave()
	if err != nil {
		return nil, err
	}

	return &ac, nil
}

func (ac ApplicationConfiguration) validateAppendOnly() error {
	if _, ok := validSaveOptions[strings.ToLower(ac.appendonly)]; !ok {
		return fmt.Errorf("invalid appendonly option '%s'. Only 'yes' or 'no' allowed.", ac.appendonly)
	}

	return nil
}

func (ac ApplicationConfiguration) validateSave() error {
	_, err := ac.parseSave()
	if err != nil {
		return err
	}
	return nil
}

func (ac ApplicationConfiguration) parseSave() ([]int64, error) {
	if ac.save == "" {
		return []int64{3600, 1, 300, 100, 60, 10000}, nil
	}

	split := strings.Split(ac.save, " ")
	nPairs := len(split)
	if nPairs < 2 {
		return nil, errors.New("at least 1 pair must be defined.")
	}
	if nPairs%2 != 0 {
		return nil, fmt.Errorf("save configuration must be set in pairs (<seconds> <changes>). Found %d elements.", len(split))
	}

	result := make([]int64, 0)

	for _, e := range split {
		number, err := strconv.ParseInt(e, 10, 0)
		if err != nil {
			return nil, err
		}

		result = append(result, number)
	}

	return result, nil
}
