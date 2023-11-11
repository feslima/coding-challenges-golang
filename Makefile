all: redis wc

.PHONY: redis redis-test wc clean

redis: $(filter-out *_test.go, $(wildcard redis/*.go))
	go build -o redis/redis-go -v redis/cmd

redis-test:
	go test -race -count=1 redis

wc: $(filter-out *_test.go, $(wildcard wc/*.go))
	go build -o wc/ccwc -v wc

clean:
	rm -f redis/redis-go
	rm -f wc/ccwc
