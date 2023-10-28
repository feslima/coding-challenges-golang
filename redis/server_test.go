package redis

import (
	"bufio"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

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
			want: errorResponse,
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
			want: errorResponse,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			app := NewApplication(nil)

			messenger := handleRequests(app.ProcessRequest)
			ProcessConnection(connection, &messenger)
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
	testCases := []struct {
		desc      string
		data      string
		want      []byte
		wantState map[string]StringValue
	}{
		{
			desc:      "set command",
			data:      "*3\r\n$3\r\nset\r\n$4\r\nName\r\n$4\r\nJohn\r\n",
			want:      []byte("+OK\r\n"),
			wantState: map[string]StringValue{"Name": {value: "John"}},
		},
		{
			desc:      "invalid set command",
			data:      "*3\r\n$2\r\nst\r\n$4\r\nName\r\n$4\r\nJohn\r\n",
			want:      errorResponse,
			wantState: map[string]StringValue{},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			connection := NewConnection(tC.data)
			app := NewApplication(nil)

			messenger := handleRequests(app.ProcessRequest)
			ProcessConnection(connection, &messenger)
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
			app := NewApplication(nil)
			app.state.stringMap = tC.state

			messenger := handleRequests(app.ProcessRequest)
			ProcessConnection(connection, &messenger)
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
