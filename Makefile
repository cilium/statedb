.PHONY: all build test test-race bench bench-reconciler

all: build test test-race bench bench-reconciler

build:
	go build ./...

test:
	go test ./... -cover -test.count 1

test-race:
	go test -race ./... -test.count 1

bench:
	go test ./... -bench . -test.run xxx

bench-reconciler:
	go run ./reconciler/benchmark
