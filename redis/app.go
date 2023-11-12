package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
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

type ApplicationClient struct {
	conn              net.Conn
	isOnSubscribeMode bool
}

type Application struct {
	state   *ApplicationState
	config  *ApplicationConfiguration
	logger  *slog.Logger
	clock   ClockTimer
	clients map[string]*ApplicationClient
}

func NewApplication(config *ApplicationConfiguration, timer ClockTimer, l *slog.Logger) *Application {
	mutex := &sync.RWMutex{}
	state := ApplicationState{
		keyspace: *newKeyspace(timer, mutex),
		mutex:    mutex,
	}
	return &Application{
		state:   &state,
		config:  config,
		clock:   timer,
		logger:  l,
		clients: make(map[string]*ApplicationClient),
	}
}

func (app *Application) AddClient(c net.Conn) {
	addr := c.RemoteAddr().String()
	parsed := net.ParseIP(addr)
	if parsed == nil {
		// TODO: better handle this silent failure
		// leaving like this because we need to use net.Pipe in testing
		app.logger.Error(fmt.Sprintf("invalid ip address '%s'", addr))
		return
	}

	app.clients[parsed.To4().String()] = &ApplicationClient{
		conn:              c,
		isOnSubscribeMode: false,
	}
}

func (app *Application) ProcessRequest(m Message) ([]byte, error) {
	command, err := DecodeMessage(m.raw, app)
	if err != nil {
		app.logger.Error("error decoding message: " + fmt.Sprintf("%s", err))
		return []byte{}, err
	}
	response, err := command.Process()
	if err != nil {
		app.logger.Error("error parsing message: " + fmt.Sprintf("%s", err))
		return []byte{}, err
	}

	return []byte(response), nil
}

type ApplicationState struct {
	mutex    *sync.RWMutex
	keyspace keyspace
}

func (as *ApplicationState) ResetCounter() {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.keyspace.modifications = 0
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
		cmd, err := DecodeMessage(line, a)
		if err != nil {
			continue
		}
		err = cmd.Parse()
		if err != nil {
			continue
		}

		_, err = cmd.Process()
		if err != nil {
			continue
		}
	}

	as.ResetCounter()
	return nil
}

func (app *Application) LoadStateFromSnapshot() {
	if _, err := os.Stat("redis-go.rdb"); err == nil {
		f, err := os.Open("redis-go.rdb")
		if err == nil {
			app.logger.Info("loading previous state from snapshot")
			err = app.state.Load(f, app)
			f.Close()
			if err == nil {
				app.logger.Info("done loading snapshot")
			} else {
				app.logger.Info("failed to load state from snapshot. Proceeding with empty state")
			}
		}
	}
}

func (app *Application) SetupSnapshotSavers() func() {
	var closerFuncs []func()
	for i := 0; i < len(app.config.Save); i += 2 {
		seconds := app.config.Save[i]
		changes := app.config.Save[i+1]
		cs := RunEveryNSeconds(time.Duration(seconds)*time.Second, func() { SaveAfterNChanges(changes, app) })
		closerFuncs = append(closerFuncs, cs)
	}

	closeSavers := func() {
		for _, closer := range closerFuncs {
			closer()
		}
	}
	return closeSavers
}

func (app *Application) SetupKeyExpirer() func() {
	return RunEveryNSeconds(time.Second/10, func() { CheckAndExpireKeys(app) })
}

func SaveAfterNChanges(n int64, app *Application) {
	app.state.mutex.RLock()
	modifications := int64(app.state.keyspace.modifications)
	app.state.mutex.RUnlock()

	if modifications >= n {
		app.logger.Info(fmt.Sprintf("saving snapshot after %d changes...", modifications))
		f, err := os.Create("redis-go.rdb")
		if err != nil {
			app.logger.Error("failed to open redis-go.rdb file")
			return
		}
		defer f.Close()

		err = app.state.Save(f)
		if err != nil {
			app.logger.Error("failed to save snapshot")
			return
		}
		app.logger.Info("finished saving snapshot...")
	}
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
	Save       []int64
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

func (ac *ApplicationConfiguration) validateSave() error {
	p, err := ac.parseSave()
	if err != nil {
		return err
	}
	ac.Save = p
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
