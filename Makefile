all: redis wc

.PHONY: redis wc clean

redis: $(filter-out *_test.go, $(wildcard redis/*.go))
	go build -o redis/redis-go -v redis

wc: $(filter-out *_test.go, $(wildcard wc/*.go))
	go build -o wc/ccwc -v wc

clean:
	rm -f redis/redis-go
	rm -f wc/ccwc
