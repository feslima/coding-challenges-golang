package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
)

// Creates a net.Listener on success. You are responsible for closing
// this Listener.
func NewServer(host string, port int, l *slog.Logger) (net.Listener, error) {
	p := fmt.Sprintf("%04d", port)
	server, err := net.Listen("tcp", host+":"+p)
	if err != nil {
		return nil, err
	}
	l.Info("Initialized server " + host + ":" + p)
	return server, err
}

type ConnectionHandler func(Message) ([]byte, error)

func Listen(server net.Listener, app *Application, l *slog.Logger) {
	messenger := &messenger{
		app:  app,
		in:   make(chan Message),
		done: make(chan struct{}),
	}
	go messenger.handleRequests()

	for {
		conn, err := server.Accept()
		if err != nil {
			l.Error("failed to accept connection")
			continue
		}

		err = app.AddClient(conn, true)
		if err != nil {
			l.Error(fmt.Sprintf("failed to add client connection: %v", err))
			conn.Write([]byte(SerializeSimpleError(err.Error())))
			continue
		}

		go HandleConnection(conn, messenger, l)
	}
}

var errorResponse []byte = []byte("-couldn't process request\r\n")

type messenger struct {
	app  *Application
	in   chan Message
	done chan struct{}
}

func (m *messenger) Cancel() func() {
	return func() { close(m.done) }
}

func (messenger *messenger) handleRequests() {
	l := messenger.app.logger
	for {
		select {
		case <-messenger.done:
			break
		case m := <-messenger.in:
			response, err := messenger.app.ProcessRequest(m)
			if err != nil {
				l.Error(fmt.Sprintf("%v", err))

				_, err = m.conn.Write([]byte(SerializeSimpleError(err.Error())))
				if err != nil {
					l.Error(fmt.Sprintf("%v", err))
				}
				continue
			}

			if response == nil {
				l.Error("got nil response struct")
				continue
			}

			for _, c := range response.targets {
				if c == nil {
					l.Error("got a nil connection object")
					continue
				}
				_, err = c.Write(response.message)
				if err != nil {
					l.Error("failed to write error response")
					continue
				}
			}
		}
	}
}

type Message struct {
	raw  []byte
	conn net.Conn
}

func HandleConnection(conn net.Conn, m *messenger, l *slog.Logger) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	buf := make([]byte, reader.Size())

	for {
		n, err := reader.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				l.Debug("EOF error. Client disconnected. No more data to read.")
				break
			}

			l.Error("failed to read bytes: " + fmt.Sprintf("%v", err))
			_, err = conn.Write(errorResponse)
			if err != nil {
				l.Error("failed to write error response")
			}

			continue
		}

		read := buf[:n]
		l.Debug("received: " + string(read))

		select {
		case <-m.done:
			break
		case m.in <- Message{raw: read, conn: conn}:
		}
	}
}
