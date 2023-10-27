package redis

import (
	"fmt"
	"log/slog"
)

type Application struct {
	state *ApplicationState
}

func NewApplication() *Application {
	state := ApplicationState{stringMap: make(map[string]string)}
	return &Application{state: &state}
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

type ApplicationState struct {
	stringMap map[string]string
}
