.DEFAULT_GOAL := help

.PHONY: help build proto clean

help:
	@echo "Available targets:"
	@echo "  build   - Build node and demo binaries"
	@echo "  proto   - Generate protobuf and gRPC code"
	@echo "  clean   - Remove binaries and temp files"

build:
	go build -o bin/node ./cmd/node/

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/raft.proto

clean:
	rm -rf bin/ /tmp/raft-node-* /tmp/raft-demo-*
