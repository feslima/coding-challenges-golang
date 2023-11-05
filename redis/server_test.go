package redis

import (
	"bufio"
	"bytes"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

var testLogOpts = slog.HandlerOptions{
	Level: slog.LevelDebug,
}

func NewTestLogger() *slog.Logger {
	logBuf := bytes.NewBuffer(make([]byte, 1024))
	logHandler := slog.NewTextHandler(logBuf, &testLogOpts)
	return slog.New(logHandler)
}

type TestClockTimer struct {
	mockNow time.Time
}

func (c TestClockTimer) Now() time.Time {
	return c.mockNow
}

type ConnectionTester struct {
	request        *bufio.Reader
	response       []byte
	deadline       time.Time
	closeCallCount int
}

func NewConnection(data string) *ConnectionTester {
	buf := strings.NewReader(data)
	connection := &ConnectionTester{
		request:        bufio.NewReader(buf),
		response:       nil,
		closeCallCount: 0,
	}
	return connection
}

func (c *ConnectionTester) Read(b []byte) (int, error) {
	return c.request.Read(b)
}

func (c *ConnectionTester) Write(b []byte) (int, error) {
	c.response = b
	return len(b), nil
}

func (c *ConnectionTester) Close() error {
	c.closeCallCount += 1
	return nil
}

func (c *ConnectionTester) LocalAddr() net.Addr {
	return nil
}

func (c *ConnectionTester) RemoteAddr() net.Addr {
	return nil
}

func (c *ConnectionTester) SetDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

func (c *ConnectionTester) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *ConnectionTester) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestReadonlyCommands(t *testing.T) {
	testCases := []struct {
		desc string
		data string
		want []byte
	}{
		{
			desc: "ping command",
			data: "*1\r\n$4\r\nping\r\n",
			want: []byte("+PONG\r\n"),
		},
		{
			desc: "invalid ping command",
			data: "*1\r\n$4\r\npang\r\n",
			want: []byte("-invalid command: 'pang'\r\n"),
		},
		{
			desc: "echo command",
			data: "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n",
			want: []byte("$11\r\nhello world\r\n"),
		},
		{
			desc: "empty echo command",
			data: "*2\r\n$4\r\necho\r\n$0\r\n\r\n",
			want: []byte("$0\r\n\r\n"),
		},
		{
			desc: "invalid echo command",
			data: "*1\r\n$4\r\necho\r\n",
			want: []byte("-wrong number of arguments.\r\n"),
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: time.Now()}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			got := connection.response

			if connection.closeCallCount != 1 {
				t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
			}

			if !reflect.DeepEqual(got, tC.want) {
				t.Errorf("got: %#v. want: %#v", string(got), string(tC.want))
			}
		})
	}
}

type mapState struct {
	ks map[string]keyspaceEntry
	sm map[string]string
	lm map[string]list
}

type testCase struct {
	now          time.Time
	desc         string
	data         string
	want         []byte
	initialState mapState
	wantState    mapState
}

func setupAppAndConnection(tC testCase) (*ConnectionTester, *Application, *slog.Logger) {
	connection := NewConnection(tC.data)
	timer := TestClockTimer{mockNow: tC.now}
	logger := NewTestLogger()
	app := NewApplication(nil, timer, logger)
	app.state.keyspace.keys = tC.initialState.ks
	app.state.keyspace.stringMap = tC.initialState.sm
	app.state.keyspace.listMap = tC.initialState.lm

	return connection, app, logger
}

func assertConnectionAndAppState(t *testing.T, tC testCase, connection *ConnectionTester, app *Application) {
	got := connection.response

	if connection.closeCallCount != 1 {
		t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
	}

	if !reflect.DeepEqual(got, tC.want) {
		t.Errorf("got: %#v. want: %#v", string(got), string(tC.want))
	}

	gotState := app.state
	gotKs := gotState.keyspace
	gotSmap := gotKs.stringMap
	gotLmap := gotKs.listMap

	if !reflect.DeepEqual(gotKs.keys, tC.wantState.ks) {
		t.Errorf("got: %#v. want: %#v", gotKs, tC.wantState.ks)
	}

	if !reflect.DeepEqual(gotSmap, tC.wantState.sm) {
		t.Errorf("got: %#v. want: %#v", gotSmap, tC.wantState.sm)
	}

	if !reflect.DeepEqual(gotLmap, tC.wantState.lm) {
		t.Errorf("got: %#v. want: %#v", gotLmap, tC.wantState.lm)
	}
}

func TestSetCommand(t *testing.T) {
	now := time.Now()
	testCases := []testCase{
		{
			now:  now,
			desc: "invalid set command",
			data: "*3\r\n$2\r\nst\r\n$4\r\nName\r\n$4\r\nJohn\r\n",
			want: []byte("-invalid command: 'st'\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "set string key",
			data: "*3\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n",
			want: []byte(OK_SIMPLE_STRING),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "set in existing key with list should change keyspace",
			data: "*3\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n",
			want: []byte(OK_SIMPLE_STRING),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestGetCommand(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "get existing string key",
			data: "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want: []byte("$4\r\nJohn\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "get non existing string key",
			data: "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want: []byte(NIL_BULK_STRING),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "get on existing list key should return as non existing",
			data: "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want: []byte(NIL_BULK_STRING),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func getFuture(now time.Time, delta int) *time.Time {
	future := now.Add(time.Duration(delta) * time.Second)
	return &future
}

func TestSetWithExpiryCommand(t *testing.T) {
	now := time.Now()
	future := getFuture(now, 2)

	testCases := []testCase{
		{
			now:  now,
			desc: "set command with expiry in seconds",
			data: "*5\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n$2\r\nex\r\n$1\r\n2\r\n",
			want: []byte(OK_SIMPLE_STRING),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: future}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "set command with expiry in milliseconds",
			data: "*5\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n$2\r\npx\r\n$4\r\n2000\r\n",
			want: []byte(OK_SIMPLE_STRING),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: future}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestActiveKeyExpiration(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "should delete key if it is expired on get",
			data: "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want: []byte(NIL_BULK_STRING),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: getFuture(now, -2)}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "should not delete key if it is not expired on get",
			data: "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want: []byte("$4\r\nJohn\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: getFuture(now, 2)}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: getFuture(now, 2)}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestExpireCommand(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "expire on persistent key",
			data: "*3\r\n$6\r\nexpire\r\n$4\r\nName\r\n$1\r\n1\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: getFuture(now, 1)}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "expire on volatile key should update time",
			data: "*3\r\n$6\r\nexpire\r\n$4\r\nName\r\n$1\r\n1\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: getFuture(now, 1)}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: getFuture(now, 2)}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
		{
			now:  now,
			desc: "expire on non-existant key should do nothing",
			data: "*3\r\n$6\r\nexpire\r\n$7\r\nUnknown\r\n$1\r\n1\r\n",
			want: []byte(":0\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestExistsCommand(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "existing key single time",
			data: "*2\r\n$6\r\nexists\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "existing key repeated",
			data: "*3\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nName\r\n",
			want: []byte(":2\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
		{
			now:  now,
			desc: "non existing key single time",
			data: "*2\r\n$6\r\nexists\r\n$4\r\nNone\r\n",
			want: []byte(":0\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
		{
			now:  now,
			desc: "existing and non existing keys single time",
			data: "*3\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
		{
			now:  now,
			desc: "existing repeated and non existing single time",
			data: "*4\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nName\r\n",
			want: []byte(":2\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
		{
			now:  now,
			desc: "existing single time and non existing repeated",
			data: "*4\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
		{
			now:  now,
			desc: "existing repeated and non existing repeated",
			data: "*5\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nName\r\n$4\r\nNone\r\n",
			want: []byte(":2\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestDeleteCommand(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "delete existing key single time",
			data: "*2\r\n$3\r\ndel\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "delete existing key repeated",
			data: "*3\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "delete non existing key single time",
			data: "*2\r\n$3\r\ndel\r\n$4\r\nNone\r\n",
			want: []byte(":0\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "delete existing and non existing keys single time",
			data: "*3\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "delete existing repeated and non existing single time",
			data: "*4\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "delete existing single time and non existing repeated",
			data: "*4\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "existing repeated and non existing repeated",
			data: "*5\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nName\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "delete multiple existing keys",
			data: "*3\r\n$3\r\ndel\r\n$4\r\nName\r\n$5\r\nName2\r\n",
			want: []byte(":2\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{
					"Name":  {group: "list", expires: nil},
					"Name2": {group: "string", expires: nil},
					"Name3": {group: "list", expires: nil},
				},
				sm: map[string]string{"Name2": "John"},
				lm: map[string]list{
					"Name":  NewListFromSlice([]string{"John"}),
					"Name3": NewListFromSlice([]string{"Smith"}),
				},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name3": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name3": NewListFromSlice([]string{"Smith"})},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestIncrementCommand(t *testing.T) {
	now := time.Now()
	testCases := []testCase{
		{
			now:  now,
			desc: "increment existing key",
			data: "*2\r\n$4\r\nincr\r\n$4\r\nName\r\n",
			want: []byte(":2\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "1"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "2"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "increment non existing integer key",
			data: "*2\r\n$4\r\nincr\r\n$4\r\nName\r\n",
			want: []byte(":0\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Some": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Some": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{
					"Some": {group: "list", expires: nil},
					"Name": {group: "string", expires: nil},
				},
				sm: map[string]string{"Name": "0"},
				lm: map[string]list{"Some": NewListFromSlice([]string{"John"})},
			},
		},
		{
			now:  now,
			desc: "increment non parseable string key",
			data: "*2\r\n$4\r\nincr\r\n$4\r\nName\r\n",
			want: []byte("-key 'Name' cannot be parsed to integer\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "increment non integer key",
			data: "*2\r\n$4\r\nincr\r\n$4\r\nName\r\n",
			want: []byte("-key 'Name' does not support this operation\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestDecrementCommand(t *testing.T) {
	now := time.Now()
	testCases := []testCase{
		{
			now:  now,
			desc: "decrement existing key",
			data: "*2\r\n$4\r\ndecr\r\n$4\r\nName\r\n",
			want: []byte(":0\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "1"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "0"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "decrement non existing integer key",
			data: "*2\r\n$4\r\ndecr\r\n$4\r\nName\r\n",
			want: []byte(":0\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Some": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Some": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{
					"Some": {group: "list", expires: nil},
					"Name": {group: "string", expires: nil},
				},
				sm: map[string]string{"Name": "0"},
				lm: map[string]list{"Some": NewListFromSlice([]string{"John"})},
			},
		},
		{
			now:  now,
			desc: "decrement non parseable string key",
			data: "*2\r\n$4\r\ndecr\r\n$4\r\nName\r\n",
			want: []byte("-key 'Name' cannot be parsed to integer\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
		{
			now:  now,
			desc: "decrement non integer key",
			data: "*2\r\n$4\r\ndecr\r\n$4\r\nName\r\n",
			want: []byte("-key 'Name' does not support this operation\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"Name": NewListFromSlice([]string{"John"})},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestRPushCommand(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "push to non-existing key",
			data: "*3\r\n$5\r\nrpush\r\n$6\r\nmylist\r\n$5\r\nhello\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"mylist": NewListFromSlice([]string{"hello"})},
			},
		},
		{
			now:  now,
			desc: "push to key keeps order",
			data: "*5\r\n$5\r\nrpush\r\n$6\r\nmylist\r\n$5\r\nhello\r\n$5\r\nworld\r\n$4\r\ntest\r\n",
			want: []byte(":4\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"mylist": NewListFromSlice([]string{"hi"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"mylist": NewListFromSlice([]string{"hi", "hello", "world", "test"})},
			},
		},
		{
			now:  now,
			desc: "push to invalid existing key returns error",
			data: "*3\r\n$5\r\nrpush\r\n$6\r\nmylist\r\n$5\r\nhello\r\n",
			want: []byte("-key 'mylist' does not support this operation\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "string", expires: nil}},
				sm: map[string]string{"mylist": "hi"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "string", expires: nil}},
				sm: map[string]string{"mylist": "hi"},
				lm: map[string]list{},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestLPushCommand(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "push to non-existing key",
			data: "*3\r\n$5\r\nlpush\r\n$6\r\nmylist\r\n$5\r\nhello\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"mylist": NewListFromSlice([]string{"hello"})},
			},
		},
		{
			now:  now,
			desc: "push to key keeps order",
			data: "*5\r\n$5\r\nlpush\r\n$6\r\nmylist\r\n$5\r\nhello\r\n$5\r\nworld\r\n$4\r\ntest\r\n",
			want: []byte(":4\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"mylist": NewListFromSlice([]string{"hi"})},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "list", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{"mylist": NewListFromSlice([]string{"test", "world", "hello", "hi"})},
			},
		},
		{
			now:  now,
			desc: "push to invalid existing key returns error",
			data: "*3\r\n$5\r\nlpush\r\n$6\r\nmylist\r\n$5\r\nhello\r\n",
			want: []byte("-key 'mylist' does not support this operation\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "string", expires: nil}},
				sm: map[string]string{"mylist": "hi"},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "string", expires: nil}},
				sm: map[string]string{"mylist": "hi"},
				lm: map[string]list{},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)
		})
	}
}

func TestChangesCounting(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "should count on write operation",
			data: "*3\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n",
			want: []byte(OK_SIMPLE_STRING),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"Name": {group: "string", expires: nil}},
				sm: map[string]string{"Name": "John"},
				lm: map[string]list{},
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection, app, logger := setupAppAndConnection(tC)

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			assertConnectionAndAppState(t, tC, connection, app)

			if app.state.modifications != 1 {
				t.Error("expected a single write count")
			}

		})
	}
}
