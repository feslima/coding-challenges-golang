package main

import (
	"redis"
)

func main() {
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
