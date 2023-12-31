package redis

import (
	"bytes"
	"log/slog"
	"net"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/nettest"
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

func getFuture(now time.Time, delta int) *time.Time {
	future := now.Add(time.Duration(delta) * time.Second)
	return &future
}

type rbtState struct {
	tree   rbtree[float64, string]
	keys   []float64
	values []string
}

type mapState struct {
	ks map[string]keyspaceEntry
	sm map[string]string
	lm map[string]list
	tm map[string]rbtState
}

type caseTesterSetup interface {
	Now() time.Time
	InitialState() mapState
}

type testCase struct {
	now          time.Time
	desc         string
	data         string
	want         []byte
	initialState mapState
	wantState    mapState
}

func (t testCase) Now() time.Time {
	return t.now
}

func (t testCase) InitialState() mapState {
	return t.initialState
}

func setupApplication(tC caseTesterSetup, t *testing.T) (*Application, net.Listener, *slog.Logger) {
	timer := TestClockTimer{mockNow: tC.Now()}
	logger := NewTestLogger()
	app := NewApplication(nil, timer, logger)
	initialState := tC.InitialState()
	app.state.keyspace.keys = initialState.ks
	app.state.keyspace.stringMap = initialState.sm
	app.state.keyspace.listMap = initialState.lm
	app.state.keyspace.sortedSetMap = func() map[string]rbtree[float64, string] {
		m := make(map[string]rbtree[float64, string], 0)
		for k, v := range initialState.tm {
			m[k] = v.tree
		}
		return m
	}()

	srv, err := nettest.NewLocalListener("tcp")
	if err != nil {
		t.Fatalf("failed to setup listener: %v", err)
	}

	return app, srv, logger
}

func makeRequestToServer(data string, srv net.Listener, t *testing.T) net.Conn {
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("could not establish connection: %v", err)
	}

	if _, err := conn.Write([]byte(data)); err != nil {
		t.Fatalf("could not write payload to server: %v", err)
	}
	return conn
}

func assertConnectionAndAppState(t *testing.T, tC testCase, connection net.Conn, app *Application) {
	buf := make([]byte, 4096)
	n, err := connection.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from connection: %s", err)
	}
	got := buf[:n]

	if !reflect.DeepEqual(got, tC.want) {
		t.Errorf("got: %#v. want: %#v", string(got), string(tC.want))
	}

	gotState := app.state
	gotKs := gotState.keyspace
	gotSmap := gotKs.stringMap
	gotLmap := gotKs.listMap
	gotSSmap := gotKs.sortedSetMap

	if !reflect.DeepEqual(gotKs.keys, tC.wantState.ks) {
		t.Errorf("got: %#v. want: %#v", gotKs, tC.wantState.ks)
	}

	if !reflect.DeepEqual(gotSmap, tC.wantState.sm) {
		t.Errorf("got: %#v. want: %#v", gotSmap, tC.wantState.sm)
	}

	if !reflect.DeepEqual(gotLmap, tC.wantState.lm) {
		t.Errorf("got: %#v. want: %#v", gotLmap, tC.wantState.lm)
	}

	for k, wantSSet := range tC.wantState.tm {
		gotSSet, ok := gotSSmap[k]
		if !ok {
			t.Errorf("sorted key '%s' not found", k)
		}

		gotSSetKs := gotSSet.GetKeySet()
		wantSSetKs := wantSSet.keys
		if !reflect.DeepEqual(gotSSetKs, wantSSetKs) {
			t.Errorf("keys set - got: %#v. want: %#v", gotSSetKs, wantSSetKs)
		}

		gotSSetVs := gotSSet.GetValueSet()
		wantSSetVs := wantSSet.values
		if !reflect.DeepEqual(gotSSetVs, wantSSetVs) {
			t.Errorf("values set - got: %#v. want: %#v", gotSSetVs, wantSSetVs)
		}

	}
}

func TestReadonlyCommands(t *testing.T) {
	now := time.Now()
	initialState := mapState{
		ks: map[string]keyspaceEntry{},
		sm: map[string]string{},
		lm: map[string]list{},
	}
	wantState := mapState{
		ks: map[string]keyspaceEntry{},
		sm: map[string]string{},
		lm: map[string]list{},
	}

	testCases := []testCase{
		{
			now:          now,
			desc:         "ping command",
			data:         "*1\r\n$4\r\nping\r\n",
			want:         []byte("+PONG\r\n"),
			initialState: initialState,
			wantState:    wantState,
		},
		{
			now:          now,
			desc:         "invalid ping command",
			data:         "*1\r\n$4\r\npang\r\n",
			want:         []byte("-invalid command: 'pang'\r\n"),
			initialState: initialState,
			wantState:    wantState,
		},
		{
			now:          now,
			desc:         "echo command",
			data:         "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n",
			want:         []byte("$11\r\nhello world\r\n"),
			initialState: initialState,
			wantState:    wantState,
		},
		{
			now:          now,
			desc:         "empty echo command",
			data:         "*2\r\n$4\r\necho\r\n$0\r\n\r\n",
			want:         []byte("$0\r\n\r\n"),
			initialState: initialState,
			wantState:    wantState,
		},
		{
			now:          now,
			desc:         "invalid echo command",
			data:         "*1\r\n$4\r\necho\r\n",
			want:         []byte("-wrong number of arguments.\r\n"),
			initialState: initialState,
			wantState:    wantState,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
		})
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
		})
	}
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
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
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)

			mods := app.state.keyspace.modifications
			if mods != 1 {
				t.Error("expected a single write count")
			}
		})
	}
}

func TestZAddCommand(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "push to non-existing key",
			data: "*4\r\n$4\r\nzadd\r\n$5\r\nmyset\r\n$1\r\n1\r\n$5\r\nNorem\r\n",
			want: []byte(":1\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
				tm: map[string]rbtState{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"myset": {group: "sorted-set", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{},
				tm: func() map[string]rbtState {
					tree := NewTree[float64, string]()
					tree.Put(1, "Norem")

					sset := make(map[string]rbtState)
					sset["myset"] = rbtState{
						tree:   *tree,
						keys:   []float64{1},
						values: []string{"Norem"},
					}
					return sset
				}(),
			},
		},
		{
			now:  now,
			desc: "push to invalid existing key returns error",
			data: "*4\r\n$4\r\nzadd\r\n$6\r\nmylist\r\n$1\r\n1\r\n$5\r\nNorem\r\n",
			want: []byte("-key 'mylist' does not support this operation\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "string", expires: nil}},
				sm: map[string]string{"mylist": "hi"},
				lm: map[string]list{},
				tm: map[string]rbtState{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"mylist": {group: "string", expires: nil}},
				sm: map[string]string{"mylist": "hi"},
				lm: map[string]list{},
				tm: map[string]rbtState{},
			},
		},
		{
			now:  now,
			desc: "push keeps correct order",
			data: "*14\r\n$4\r\nzadd\r\n$5\r\nmyset\r\n$2\r\n10\r\n$5\r\nNorem\r\n$2\r\n12\r\n$8\r\nCastilla\r\n$1\r\n8\r\n$10\r\nSam-Bodden\r\n$2\r\n10\r\n$5\r\nRoyce\r\n$1\r\n6\r\n$4\r\nFord\r\n$2\r\n14\r\n$8\r\nPrickett\r\n",
			want: []byte(":6\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{},
				sm: map[string]string{},
				lm: map[string]list{},
				tm: map[string]rbtState{},
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"myset": {group: "sorted-set", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{},
				tm: func() map[string]rbtState {
					tree := NewTree[float64, string]()
					tree.Put(10, "Norem")
					tree.Put(12, "Castilla")
					tree.Put(8, "Sam-Bodden")
					tree.Put(10, "Royce")
					tree.Put(6, "Ford")
					tree.Put(14, "Prickett")

					sset := make(map[string]rbtState)
					sset["myset"] = rbtState{
						tree:   *tree,
						keys:   []float64{6, 8, 10, 10, 12, 14},
						values: []string{"Ford", "Sam-Bodden", "Norem", "Royce", "Castilla", "Prickett"},
					}
					return sset
				}(),
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
		})
	}
}

func TestZRangeCommand(t *testing.T) {
	now := time.Now()

	testCases := []testCase{
		{
			now:  now,
			desc: "get all elements",
			data: "*4\r\n$6\r\nzrange\r\n$5\r\nmyset\r\n$1\r\n0\r\n$2\r\n-1\r\n",
			want: []byte("*6\r\n$4\r\nFord\r\n$10\r\nSam-Bodden\r\n$5\r\nNorem\r\n$5\r\nRoyce\r\n$8\r\nCastilla\r\n$8\r\nPrickett\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"myset": {group: "sorted-set", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{},
				tm: func() map[string]rbtState {
					tree := NewTree[float64, string]()
					tree.Put(10, "Norem")
					tree.Put(12, "Castilla")
					tree.Put(8, "Sam-Bodden")
					tree.Put(10, "Royce")
					tree.Put(6, "Ford")
					tree.Put(14, "Prickett")

					sset := make(map[string]rbtState)
					sset["myset"] = rbtState{
						tree:   *tree,
						keys:   []float64{6, 8, 10, 10, 12, 14},
						values: []string{"Ford", "Sam-Bodden", "Norem", "Royce", "Castilla", "Prickett"},
					}
					return sset
				}(),
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"myset": {group: "sorted-set", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{},
				tm: func() map[string]rbtState {
					tree := NewTree[float64, string]()
					tree.Put(10, "Norem")
					tree.Put(12, "Castilla")
					tree.Put(8, "Sam-Bodden")
					tree.Put(10, "Royce")
					tree.Put(6, "Ford")
					tree.Put(14, "Prickett")

					sset := make(map[string]rbtState)
					sset["myset"] = rbtState{
						tree:   *tree,
						keys:   []float64{6, 8, 10, 10, 12, 14},
						values: []string{"Ford", "Sam-Bodden", "Norem", "Royce", "Castilla", "Prickett"},
					}
					return sset
				}(),
			},
		},
		{
			now:  now,
			desc: "get some elements elements",
			data: "*4\r\n$6\r\nzrange\r\n$5\r\nmyset\r\n$1\r\n1\r\n$2\r\n-2\r\n",
			want: []byte("*4\r\n$10\r\nSam-Bodden\r\n$5\r\nNorem\r\n$5\r\nRoyce\r\n$8\r\nCastilla\r\n"),
			initialState: mapState{
				ks: map[string]keyspaceEntry{"myset": {group: "sorted-set", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{},
				tm: func() map[string]rbtState {
					tree := NewTree[float64, string]()
					tree.Put(10, "Norem")
					tree.Put(12, "Castilla")
					tree.Put(8, "Sam-Bodden")
					tree.Put(10, "Royce")
					tree.Put(6, "Ford")
					tree.Put(14, "Prickett")

					sset := make(map[string]rbtState)
					sset["myset"] = rbtState{
						tree:   *tree,
						keys:   []float64{6, 8, 10, 10, 12, 14},
						values: []string{"Ford", "Sam-Bodden", "Norem", "Royce", "Castilla", "Prickett"},
					}
					return sset
				}(),
			},
			wantState: mapState{
				ks: map[string]keyspaceEntry{"myset": {group: "sorted-set", expires: nil}},
				sm: map[string]string{},
				lm: map[string]list{},
				tm: func() map[string]rbtState {
					tree := NewTree[float64, string]()
					tree.Put(10, "Norem")
					tree.Put(12, "Castilla")
					tree.Put(8, "Sam-Bodden")
					tree.Put(10, "Royce")
					tree.Put(6, "Ford")
					tree.Put(14, "Prickett")

					sset := make(map[string]rbtState)
					sset["myset"] = rbtState{
						tree:   *tree,
						keys:   []float64{6, 8, 10, 10, 12, 14},
						values: []string{"Ford", "Sam-Bodden", "Norem", "Royce", "Castilla", "Prickett"},
					}
					return sset
				}(),
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			app, srv, logger := setupApplication(tC, t)

			go func() { Listen(srv, app, logger) }()

			conn := makeRequestToServer(tC.data, srv, t)
			defer conn.Close()

			assertConnectionAndAppState(t, tC, conn, app)
		})
	}
}
