package redis

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
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
	logger *slog.Logger
	clock  ClockTimer
}

func NewApplication(config *ApplicationConfiguration, timer ClockTimer, l *slog.Logger) *Application {
	state := ApplicationState{
		stringMap: make(map[string]StringValue),
		mutex:     &sync.RWMutex{},
	}
	return &Application{
		state:  &state,
		config: config,
		clock:  timer,
		logger: l,
	}
}

func (app *Application) ProcessRequest(raw []byte) ([]byte, error) {
	command, err := DecodeMessage(raw)
	if err != nil {
		app.logger.Error("error decoding message: " + fmt.Sprintf("%v", err))
		return []byte{}, err
	}
	response, err := command.Process(app)
	if err != nil {
		app.logger.Error("error parsing message: " + fmt.Sprintf("%v", err))
		return []byte{}, err
	}

	return []byte(response), nil
}

type StringValue struct {
	value   string
	expires *time.Time
}

type ApplicationState struct {
	stringMap map[string]StringValue
	mutex     *sync.RWMutex
}

func CheckAndExpireKeys(app *Application) {
	state := app.state
	state.mutex.RLock()
	keys := GetKeys(state.stringMap, func(sv StringValue) bool { return CheckIsExpired(app.clock, sv) })
	state.mutex.RUnlock()

	nKeys := len(keys)
	if nKeys != 0 {
		app.logger.Info(fmt.Sprintf("deleting %d expired keys", nKeys))

		state.mutex.Lock()
		for _, key := range keys {
			delete(state.stringMap, key)
		}
		state.mutex.Unlock()
	}
}

func GetKeys[K comparable, V any](m map[K]V, filter func(V) bool) []K {
	keys := make([]K, 0, len(m))

	for k := range m {
		isValid := filter(m[k])
		if isValid {
			keys = append(keys, k)
		}
	}

	return keys
}

func CheckIsExpired(c ClockTimer, sv StringValue) bool {
	if sv.expires == nil {
		return false
	}

	expires := *sv.expires
	return c.Now().After(expires)
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

func RunEveryNSeconds(d time.Duration, runner func()) func() {
	ticker := time.NewTicker(d)
	done := make(chan struct{})
	stopFunc := func() { close(done) }

	go func() {
		var wg sync.WaitGroup
		for {
			select {
			case <-ticker.C:
				wg.Add(1)
				go func() {
					runner()
					wg.Done()
				}()
			case <-done:
				break
			}
		}
		wg.Wait()
	}()

	return stopFunc
}
