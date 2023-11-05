package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
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
	mutex := &sync.RWMutex{}
	state := ApplicationState{
		keyspace:      *newKeyspace(timer, mutex),
		mutex:         mutex,
		modifications: 0,
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

type ApplicationState struct {
	mutex         *sync.RWMutex
	keyspace      keyspace
	modifications int
}

func (as *ApplicationState) CountModification() {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.modifications += 1
}

func (as *ApplicationState) ResetCounter() {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.modifications = 0
}

func (as *ApplicationState) Save(out io.Writer) error {
	as.mutex.RLock()

	for k, v := range as.keyspace.stringMap {
		e := as.keyspace.keys[k]

		kv := fmt.Sprintf("%s%s", SerializeBulkString(k), SerializeBulkString(v))
		cmd := fmt.Sprintf("*3\r\n$3\r\nset\r\n%s", kv)
		fmt.Fprint(out, cmd)

		if e.expires != nil {
			exp := e.expires.Unix()
			cmd = fmt.Sprintf("*3\r\n$8\r\nexpireat\r\n%s$%d\r\n%d\r\n", SerializeBulkString(k), len(fmt.Sprint(exp)), exp)

			fmt.Fprint(out, cmd)
		}
	}

	for k, v := range as.keyspace.listMap {
		e := as.keyspace.keys[k]

		if v.size > 0 {
			result := fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
			for _, d := range v.ToSlice() {
				string := SerializeBulkString(d)
				result += string
			}
			cmd := fmt.Sprintf("*%d\r\n$5\r\nrpush\r\n%s", v.size+2, result)
			fmt.Fprint(out, cmd)

			if e.expires != nil {
				exp := e.expires.Unix()
				cmd = fmt.Sprintf("*3\r\n$8\r\nexpireat\r\n%s$%d\r\n%d\r\n", SerializeBulkString(k), len(fmt.Sprint(exp)), exp)

				fmt.Fprint(out, cmd)
			}
		}
	}

	as.mutex.RUnlock()

	as.ResetCounter()
	return nil
}

func splitByBulkArray(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Return nothing if at end of file and no data passed
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// Find the index of the input of a newline followed by an
	// asterisk sign.
	if i := strings.Index(string(data), "\n*"); i >= 0 {
		return i + 1, data[0 : i+1], nil
	}

	// If at end of file with data return the data
	if atEOF {
		return len(data), data, nil
	}
	return
}

func (as *ApplicationState) Load(r io.Reader, a *Application) error {
	s := bufio.NewScanner(r)
	s.Split(splitByBulkArray)

	for s.Scan() {
		line := s.Bytes()
		cmd, err := DecodeMessage(line)
		if err != nil {
			continue
		}
		err = cmd.Parse()
		if err != nil {
			continue
		}

		_, err = cmd.Process(a)
		if err != nil {
			continue
		}
	}

	as.ResetCounter()
	return nil
}

func openFile(filename string) (*os.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func CheckAndExpireKeys(app *Application) {
	state := app.state
	state.mutex.RLock()
	keys := GetKeys(state.keyspace.keys, func(ke keyspaceEntry) bool { return CheckIsExpired(app.clock, ke) })
	state.mutex.RUnlock()

	nKeys := len(keys)
	if nKeys != 0 {
		app.logger.Info(fmt.Sprintf("deleting %d expired keys", nKeys))

		app.state.keyspace.BulkDelete(keys)
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
