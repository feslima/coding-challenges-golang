package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"redis"
	"strings"
)

func main() {
	programName := os.Args[0]
	args := os.Args[1:]

	c, err := NewConfigs(programName, args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	logOpts := &slog.HandlerOptions{
		Level: c.LogLevel,
	}
	logHandler := slog.NewTextHandler(os.Stderr, logOpts)
	logger := slog.New(logHandler)

	server, err := redis.NewServer(c.Host, c.Port, logger)
	if err != nil {
		panic(err)
	}
	defer server.Close()

	config, err := redis.NewApplicationConfiguration("no", "3600 1 300 100 60 10000")
	if err != nil {
		panic(err)
	}

	timer := redis.RealClockTimer{}
	app := redis.NewApplication(config, timer, logger)

	app.LoadStateFromSnapshot()
	app.SetupSnapshotSavers()
	app.SetupKeyExpirer()

	redis.Listen(server, app, logger)
}

type configs struct {
	Host     string
	Port     int
	LogLevel slog.Level
}

func NewConfigs(programName string, args []string) (*configs, error) {
	c := configs{
		Host:     "localhost",
		LogLevel: slog.LevelInfo,
	}

	err := c.Parse(programName, args)
	if err != nil {
		return nil, err
	}

	return &c, nil

}

func (c *configs) Parse(programName string, args []string) error {
	flags := flag.NewFlagSet(programName, flag.ContinueOnError)
	flags.Func("h", "host address", func(s string) error {
		parsed := net.ParseIP(s)
		if parsed == nil {
			return fmt.Errorf("invalid host ip address '%s'", s)
		}
		c.Host = s
		return nil
	})

	flags.IntVar(&c.Port, "p", 6700, "host port")

	flags.Func("l", "logger level", func(s string) error {
		switch strings.ToLower(s) {
		default:
			return fmt.Errorf("invalid logger level '%s'", s)
		case "debug":
			c.LogLevel = slog.LevelDebug
		case "info":
			c.LogLevel = slog.LevelInfo
		case "warn":
			c.LogLevel = slog.LevelWarn
		case "error":
			c.LogLevel = slog.LevelError
		}

		return nil
	})

	err := flags.Parse(args)
	if err != nil {
		return err
	}

	return nil
}
