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

func TestProcessConnection(t *testing.T) {
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
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {

			connection := NewConnection(tC.data)
			ProcessConnection(connection, ProcessRequest)

			got := connection.response

			if connection.closeCallCount != 1 {
				t.Errorf("connection not closed properly. Call count %d", connection.closeCallCount)
			}

			if !reflect.DeepEqual(got, tC.want) {
				t.Errorf("got: %#v. want: %#v", got, tC.want)
			}
		})
	}
}
