# Basic implementation of a redis server in golang

What is featured in this implementation:

- Keyspace commands: GET, SET, DEL, INCR, DECR, LPUSH, RPUSH, EXISTS, EXPIRE, EXPIREAT, etc.;
- Pub/Sub commands: PUBLISH, SUBSCRIBE;
- DB persistance via snapshotting (no forking of process though);

## Intent
1. Create an almost fully compliant redis server implementation
2. Investigate on how far can golang perform compared with the official redis implementation.


## How to build
I've implemented this in golang 1.21. Also, go to the releases of this repo to get the linux-compatible binary.
`cd` into this repo root folder and run the command `make redis`. This will produce a binary called `redis-server-go` in the `redis` folder.
