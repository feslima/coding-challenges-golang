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

func Listen(server net.Listener, handler ConnectionHandler) error {
	defer server.Close()

	for {
		conn, err := server.Accept()
		if err != nil {
			slog.Error("failed to accept connection")
			return err
		}
		controller := PlaceHolderConnectionController{}

		go ProcessConnection(conn, handler, controller)
	}
}

type ConnectionController interface {
	isDone() bool
	onResponseDone()
}

type PlaceHolderConnectionController struct{}

func (cc PlaceHolderConnectionController) isDone() bool {
	return false
}

func (cc PlaceHolderConnectionController) onResponseDone() {
}

var errorResponse []byte = []byte("-couldn't process request\r\n")

func ProcessConnection(connection net.Conn, handler ConnectionHandler, controller ConnectionController) {
	defer connection.Close()
	reader := bufio.NewReader(connection)

	for {
		// just in case we want to end the connection whenever we want (e.g. testing)
		if controller.isDone() {
			return
		}

		buf := make([]byte, reader.Size())
		n, err := reader.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				controller.onResponseDone()
				slog.Warn("EOF error. No more data to read")
				break
			}

			slog.Error("failed to read bytes: " + fmt.Sprintf("%v", err))
			_, err = connection.Write(errorResponse)
			if err != nil {
				slog.Error("failed to write error response")
			}

			controller.onResponseDone()
			continue
		}
		buf = buf[:n]

		slog.Debug("received: " + string(buf))

		response, err := handler(buf)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
			_, err = connection.Write(errorResponse)
			if err != nil {
				slog.Error("failed to write error response")
			}

			controller.onResponseDone()
			continue
		}

		_, err = connection.Write(response)
		if err != nil {
			slog.Error("failed to write response")
			_, err = connection.Write(errorResponse)
			if err != nil {
				slog.Error("failed to write error response")
			}

			controller.onResponseDone()
			continue
		}

		controller.onResponseDone()
	}
}
