package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
)

func NewServer(host string, port string) (net.Listener, error) {
	server, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		return nil, err
	}
	slog.Info("Initialized server " + host + ":" + port)
	return server, err
}

type ConnectionHandler func([]byte) ([]byte, error)

func handleRequests(h ConnectionHandler) messenger {
	messenger := messenger{
		in:   make(chan []byte),
		out:  make(chan []byte),
		done: make(chan struct{}),
	}
	go func() {
		for raw := range messenger.in {
			result, err := h(raw)
			if err != nil {
				slog.Error(fmt.Sprintf("%v", err))
				messenger.out <- errorResponse
				continue
			}
			messenger.out <- result
		}
	}()

	return messenger
}

func Listen(server net.Listener, handler ConnectionHandler) error {
	defer server.Close()

	messenger := handleRequests(handler)
	for {
		conn, err := server.Accept()
		if err != nil {
			slog.Error("failed to accept connection")
			return err
		}

		go ProcessConnection(conn, &messenger)
	}
}

var errorResponse []byte = []byte("-couldn't process request\r\n")

type messenger struct {
	in   chan []byte
	out  chan []byte
	done chan struct{}
}

func (m *messenger) Cancel() func() {
	return func() { close(m.done) }
}

func ProcessConnection(conn net.Conn, m *messenger) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	buf := make([]byte, reader.Size())

	for {
		n, err := reader.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				slog.Debug("EOF error. Client disconnected. No more data to read.")
				break
			}

			slog.Error("failed to read bytes: " + fmt.Sprintf("%v", err))
			_, err = conn.Write(errorResponse)
			if err != nil {
				slog.Error("failed to write error response")
			}

			continue
		}

		read := buf[:n]
		slog.Debug("received: " + string(read))

		select {
		case <-m.done:
			break
		case m.in <- read:
		}

		for result := range m.out {
			_, err := conn.Write(result)
			if err != nil {
				slog.Error("failed to write error response")
			}
			break
		}
	}
}
