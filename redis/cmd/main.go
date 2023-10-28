package main

import (
	"log/slog"
	"os"
	"redis"
)

func main() {
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	logHandler := slog.NewTextHandler(os.Stderr, logOpts)
	slog.SetDefault(slog.New(logHandler))

	host := "localhost"
	port := "6700"

	server, err := redis.NewServer(host, port)
	if err != nil {
		panic(err)
	}

	config, err := redis.NewApplicationConfiguration("no", "3600 1 300 100 60 10000")
	if err != nil {
		panic(err)
	}

	timer := redis.RealClockTimer{}
	app := redis.NewApplication(config, timer)
	err = redis.Listen(server, app.ProcessRequest)
	if err != nil {
		panic(err)
	}
}
