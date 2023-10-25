package main

import (
	"log/slog"
	"os"
	"redis"
)

func main() {
	logHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(logHandler))

	host := "localhost"
	port := "6700"

	server, err := redis.NewServer(host, port)
	if err != nil {
		panic(err)
	}
	err = redis.Listen(server, redis.ProcessRequest)
	if err != nil {
		panic(err)
	}
}
