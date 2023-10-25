package redis

import (
	"bufio"
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

func Listen(server net.Listener, handler ConnectionHandler) error {
	defer server.Close()

	for {
		conn, err := server.Accept()
		if err != nil {
			return err
		}

		go ProcessConnection(conn, handler)
	}
}

var errorResponse []byte = []byte("-couldn't process request\r\n")

func ProcessConnection(connection net.Conn, handler ConnectionHandler) {
	defer connection.Close()

	reader := bufio.NewReader(connection)
	buf := make([]byte, reader.Size())
	n, err := reader.Read(buf)
	if err != nil {
		connection.Write(errorResponse)
	}
	buf = buf[:n]

	slog.Debug("received: " + string(buf))

	response, err := handler(buf)
	if err != nil {
		connection.Write(errorResponse)
	}

	_, err = connection.Write(response)
	if err != nil {
		connection.Write(errorResponse)
	}
}
