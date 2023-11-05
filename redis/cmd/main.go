package main

import (
	"log/slog"
	"os"
	"redis"
)

func main() {
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	logHandler := slog.NewTextHandler(os.Stderr, logOpts)
	logger := slog.New(logHandler)

	host := "localhost"
	port := "6700"

	server, err := redis.NewServer(host, port, logger)
	if err != nil {
		panic(err)
	}

	config, err := redis.NewApplicationConfiguration("no", "3600 1 300 100 60 10000")
	if err != nil {
		panic(err)
	}

	timer := redis.RealClockTimer{}
	app := redis.NewApplication(config, timer, logger)

	app.LoadStateFromSnapshot()
	closeSavers := app.SetupSnapshotSavers()
	closeChecker := app.SetupKeyExpirer()

	err = redis.Listen(server, app.ProcessRequest, logger)
	if err != nil {
		closeSavers()
		closeChecker()
		panic(err)
	}
}
