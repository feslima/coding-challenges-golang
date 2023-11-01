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

func TestSetCommand(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		desc      string
		data      string
		want      []byte
		wantState map[string]StringValue
	}{
		{
			desc: "set command",
			data: "*3\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n",
			want: []byte(OK_SIMPLE_STRING),
			wantState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
		{
			desc:      "invalid set command",
			data:      "*3\r\n$2\r\nst\r\n$4\r\nName\r\n$4\r\nJohn\r\n",
			want:      []byte("-invalid command: 'st'\r\n"),
			wantState: map[string]StringValue{},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: now}
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

			gotState := app.state.stringMap
			if !reflect.DeepEqual(gotState, tC.wantState) {
				t.Errorf("got: %#v. want: %#v", gotState, tC.wantState)
			}
		})
	}
}

func TestGetCommand(t *testing.T) {
	testCases := []struct {
		desc  string
		data  string
		want  []byte
		state map[string]StringValue
	}{
		{
			desc:  "get existing string key",
			data:  "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want:  []byte("$4\r\nJohn\r\n"),
			state: map[string]StringValue{"Name": {value: "John"}},
		},
		{
			desc:  "get non existing string key",
			data:  "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want:  []byte("$-1\r\n"),
			state: map[string]StringValue{},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: time.Now()}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)
			app.state.stringMap = tC.state

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

func TestSetWithExpiryCommand(t *testing.T) {
	now := time.Now()
	future := now.Add(2 * time.Second)

	testCases := []struct {
		desc      string
		data      string
		want      []byte
		wantState map[string]StringValue
	}{
		{
			desc: "set command with expiry in seconds",
			data: "*5\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n$2\r\nex\r\n$1\r\n2\r\n",
			want: []byte(OK_SIMPLE_STRING),
			wantState: map[string]StringValue{"Name": {
				value:   "John",
				expires: &future,
			}},
		},
		{
			desc: "set command with expiry in milliseconds",
			data: "*5\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n$2\r\npx\r\n$4\r\n2000\r\n",
			want: []byte(OK_SIMPLE_STRING),
			wantState: map[string]StringValue{"Name": {
				value:   "John",
				expires: &future,
			}},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: now}
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

			gotState := app.state.stringMap
			gotString := gotState["Name"]
			wantString := tC.wantState["Name"]

			if gotString.value != wantString.value {
				t.Errorf("got: %#v. want: %#v", gotString.value, wantString.value)
			}

			if *gotString.expires != *wantString.expires {
				t.Errorf("got: %#v. want: %#v", *gotString.expires, *wantString.expires)
			}
		})
	}
}

func TestActiveKeyExpirationTable(t *testing.T) {
	now := time.Now()
	getFuture := func(delta int) *time.Time {
		future := now.Add(time.Duration(delta) * time.Second)
		return &future
	}

	testCases := []struct {
		desc           string
		data           string
		want           []byte
		expectedDelete bool
		expires        *time.Time
	}{
		{
			desc:           "should delete key if it is expired on get",
			data:           "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want:           []byte(NIL_BULK_STRING),
			expectedDelete: true,
			expires:        getFuture(-2),
		},
		{
			desc:           "should not delete key if it is not expired on get",
			data:           "*2\r\n$3\r\nget\r\n$4\r\nName\r\n",
			want:           []byte("$4\r\nJohn\r\n"),
			expectedDelete: false,
			expires:        getFuture(2),
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			initialState := map[string]StringValue{"Name": {
				value:   "John",
				expires: tC.expires,
			}}

			timer := TestClockTimer{mockNow: now}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)
			app.state.stringMap = initialState

			connection := NewConnection("*2\r\n$3\r\nget\r\n$4\r\nName\r\n")
			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			got := connection.response

			if connection.closeCallCount != 1 {
				t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
			}

			want := tC.want
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got: %#v. want: %#v", string(got), string(want))
			}

			gotState := app.state.stringMap
			_, exists := gotState["Name"]

			if tC.expectedDelete && exists {
				t.Error("The key must not exist")
			}

			if !tC.expectedDelete && !exists {
				t.Error("The key must exist")
			}
		})
	}
}

func TestExpireCommand(t *testing.T) {
	now := time.Now()
	getFuture := func(delta int) *time.Time {
		future := now.Add(time.Duration(delta) * time.Second)
		return &future
	}

	testCases := []struct {
		desc         string
		data         string
		want         []byte
		initialState map[string]StringValue
		wantState    map[string]StringValue
	}{
		{
			desc: "expire on persistent key",
			data: "*3\r\n$6\r\nexpire\r\n$4\r\nName\r\n$1\r\n1\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
			wantState: map[string]StringValue{"Name": {
				value:   "John",
				expires: getFuture(1),
			}},
		},
		{
			desc: "expire on volatile key should update time",
			data: "*3\r\n$6\r\nexpire\r\n$4\r\nName\r\n$1\r\n1\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: getFuture(1),
			}},
			wantState: map[string]StringValue{"Name": {
				value:   "John",
				expires: getFuture(2),
			}},
		}, {
			desc: "expire on non-existant key should do nothing",
			data: "*3\r\n$6\r\nexpire\r\n$7\r\nUnknown\r\n$1\r\n1\r\n",
			want: []byte(":0\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: getFuture(1),
			}},
			wantState: map[string]StringValue{"Name": {
				value:   "John",
				expires: getFuture(1),
			}},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			timer := TestClockTimer{mockNow: now}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)
			app.state.stringMap = tC.initialState

			connection := NewConnection(tC.data)
			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			got := connection.response

			if connection.closeCallCount != 1 {
				t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
			}

			want := tC.want
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got: %#v. want: %#v", string(got), string(want))
			}

			gotState := app.state.stringMap
			gotString := gotState["Name"]
			wantString := tC.wantState["Name"]

			if gotString.value != wantString.value {
				t.Fatalf("got: %#v. want: %#v", gotString.value, wantString.value)
			}

			if *gotString.expires != *wantString.expires {
				t.Errorf("got: %#v. want: %#v", *gotString.expires, *wantString.expires)
			}
		})
	}
}

func TestExistsCommand(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		desc  string
		data  string
		want  []byte
		state map[string]StringValue
	}{
		{
			desc: "existing key single time",
			data: "*2\r\n$6\r\nexists\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			state: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
		{
			desc: "existing key repeated",
			data: "*3\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nName\r\n",
			want: []byte(":2\r\n"),
			state: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
		{
			desc: "non existing key single time",
			data: "*2\r\n$6\r\nexists\r\n$4\r\nNone\r\n",
			want: []byte(":0\r\n"),
			state: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
		{
			desc: "existing and non existing keys single time",
			data: "*3\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			state: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
		{
			desc: "existing repeated and non existing single time",
			data: "*4\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nName\r\n",
			want: []byte(":2\r\n"),
			state: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
		{
			desc: "existing single time and non existing repeated",
			data: "*4\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			state: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
		{
			desc: "existing repeated and non existing repeated",
			data: "*5\r\n$6\r\nexists\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nName\r\n$4\r\nNone\r\n",
			want: []byte(":2\r\n"),
			state: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: now}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)
			app.state.stringMap = tC.state

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

func TestDeleteCommand(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		desc         string
		data         string
		want         []byte
		initialState map[string]StringValue
		wantState    map[string]StringValue
	}{
		{
			desc: "delete existing key single time",
			data: "*2\r\n$3\r\ndel\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
			wantState: map[string]StringValue{},
		},
		{
			desc: "delete existing key repeated",
			data: "*3\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
			wantState: map[string]StringValue{},
		},
		{
			desc: "delete non existing key single time",
			data: "*2\r\n$3\r\ndel\r\n$4\r\nNone\r\n",
			want: []byte(":0\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
			wantState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
		},
		{
			desc: "delete existing and non existing keys single time",
			data: "*3\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
			wantState: map[string]StringValue{},
		},
		{
			desc: "delete existing repeated and non existing single time",
			data: "*4\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
			wantState: map[string]StringValue{},
		},
		{
			desc: "delete existing single time and non existing repeated",
			data: "*4\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
			wantState: map[string]StringValue{},
		},
		{
			desc: "existing repeated and non existing repeated",
			data: "*5\r\n$3\r\ndel\r\n$4\r\nName\r\n$4\r\nNone\r\n$4\r\nName\r\n$4\r\nNone\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{"Name": {
				value:   "John",
				expires: nil,
			}},
			wantState: map[string]StringValue{},
		},
		{
			desc: "delete multiple existing keys",
			data: "*3\r\n$3\r\ndel\r\n$4\r\nName\r\n$5\r\nName2\r\n",
			want: []byte(":2\r\n"),
			initialState: map[string]StringValue{
				"Name": {
					value:   "John",
					expires: nil,
				},
				"Name2": {
					value:   "John",
					expires: nil,
				},
				"Name3": {
					value:   "John",
					expires: nil,
				},
			},
			wantState: map[string]StringValue{"Name3": {
				value:   "John",
				expires: nil,
			}},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: now}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)
			app.state.stringMap = tC.initialState

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			got := connection.response

			if connection.closeCallCount != 1 {
				t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
			}

			if !reflect.DeepEqual(got, tC.want) {
				t.Fatalf("got: %#v. want: %#v", string(got), string(tC.want))
			}

			gotState := app.state.stringMap
			if !reflect.DeepEqual(gotState, tC.wantState) {
				t.Fatalf("got: %#v. want: %#v", gotState, tC.wantState)
			}
		})
	}
}

func TestIncrementCommand(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		desc         string
		data         string
		want         []byte
		initialState map[string]StringValue
		wantState    map[string]StringValue
	}{
		{
			desc: "increment existing key",
			data: "*2\r\n$4\r\nincr\r\n$4\r\nName\r\n",
			want: []byte(":2\r\n"),
			initialState: map[string]StringValue{
				"Name": {
					value:   "1",
					expires: nil,
				}},
			wantState: map[string]StringValue{
				"Name": {
					value:   "2",
					expires: nil,
				}},
		},
		{
			desc:         "increment non existing key",
			data:         "*2\r\n$4\r\nincr\r\n$4\r\nName\r\n",
			want:         []byte(":0\r\n"),
			initialState: map[string]StringValue{},
			wantState: map[string]StringValue{
				"Name": {
					value:   "0",
					expires: nil,
				}},
		},
		{
			desc: "increment non integer key",
			data: "*2\r\n$4\r\nincr\r\n$4\r\nName\r\n",
			want: []byte("-key cannot be parsed to integer\r\n"),
			initialState: map[string]StringValue{
				"Name": {
					value:   "not parseable",
					expires: nil,
				}},
			wantState: map[string]StringValue{
				"Name": {
					value:   "not parseable",
					expires: nil,
				}},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: now}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)
			app.state.stringMap = tC.initialState

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			got := connection.response

			if connection.closeCallCount != 1 {
				t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
			}

			if !reflect.DeepEqual(got, tC.want) {
				t.Fatalf("got: %#v. want: %#v", string(got), string(tC.want))
			}

			gotState := app.state.stringMap
			if !reflect.DeepEqual(gotState, tC.wantState) {
				t.Fatalf("got: %#v. want: %#v", gotState, tC.wantState)
			}
		})
	}
}

func TestDecrementCommand(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		desc         string
		data         string
		want         []byte
		initialState map[string]StringValue
		wantState    map[string]StringValue
	}{
		{
			desc: "decrement existing key",
			data: "*2\r\n$4\r\ndecr\r\n$4\r\nName\r\n",
			want: []byte(":1\r\n"),
			initialState: map[string]StringValue{
				"Name": {
					value:   "2",
					expires: nil,
				}},
			wantState: map[string]StringValue{
				"Name": {
					value:   "1",
					expires: nil,
				}},
		},
		{
			desc:         "decrement non existing key",
			data:         "*2\r\n$4\r\ndecr\r\n$4\r\nName\r\n",
			want:         []byte(":0\r\n"),
			initialState: map[string]StringValue{},
			wantState: map[string]StringValue{
				"Name": {
					value:   "0",
					expires: nil,
				}},
		},
		{
			desc: "decrement non integer key",
			data: "*2\r\n$4\r\ndecr\r\n$4\r\nName\r\n",
			want: []byte("-key cannot be parsed to integer\r\n"),
			initialState: map[string]StringValue{
				"Name": {
					value:   "not parseable",
					expires: nil,
				}},
			wantState: map[string]StringValue{
				"Name": {
					value:   "not parseable",
					expires: nil,
				}},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: now}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)
			app.state.stringMap = tC.initialState

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			got := connection.response

			if connection.closeCallCount != 1 {
				t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
			}

			if !reflect.DeepEqual(got, tC.want) {
				t.Fatalf("got: %#v. want: %#v", string(got), string(tC.want))
			}

			gotState := app.state.stringMap
			if !reflect.DeepEqual(gotState, tC.wantState) {
				t.Fatalf("got: %#v. want: %#v", gotState, tC.wantState)
			}
		})
	}
}

func TestRPushCommand(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		desc         string
		data         string
		want         []byte
		initialState map[string]ListValue
		wantState    map[string]ListValue
	}{
		{
			desc:         "push to non-existing key",
			data:         "*3\r\n$5\r\nrpush\r\n$6\r\nmylist\r\n$5\r\nhello\r\n",
			want:         []byte(":1\r\n"),
			initialState: map[string]ListValue{},
			wantState: map[string]ListValue{
				"mylist": {
					values:  []string{"hello"},
					expires: nil,
				}},
		},
		{
			desc: "push to key keeps order",
			data: "*5\r\n$5\r\nrpush\r\n$6\r\nmylist\r\n$5\r\nhello\r\n$5\r\nworld\r\n$4\r\ntest\r\n",
			want: []byte(":4\r\n"),
			initialState: map[string]ListValue{
				"mylist": {
					values:  []string{"hi"},
					expires: nil,
				},
			},
			wantState: map[string]ListValue{
				"mylist": {
					values:  []string{"hi", "hello", "world", "test"},
					expires: nil,
				}},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			timer := TestClockTimer{mockNow: now}
			logger := NewTestLogger()
			app := NewApplication(nil, timer, logger)
			app.state.listMap = tC.initialState

			messenger := handleRequests(app.ProcessRequest, logger)
			ProcessConnection(connection, &messenger, logger)
			messenger.Cancel()

			got := connection.response

			if connection.closeCallCount != 1 {
				t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
			}

			if !reflect.DeepEqual(got, tC.want) {
				t.Fatalf("got: %#v. want: %#v", string(got), string(tC.want))
			}

			gotState := app.state.listMap
			if !reflect.DeepEqual(gotState, tC.wantState) {
				t.Fatalf("got: %#v. want: %#v", gotState, tC.wantState)
			}
		})
	}
}
