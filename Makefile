.PHONY: test vet build

test:
	CGO_ENABLED=0 go test ./...

vet:
	CGO_ENABLED=0 go vet ./...

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -o bin/switchboardd ./cmd/switchboardd
	CGO_ENABLED=0 go build -o bin/switchboard ./cmd/switchboard
	CGO_ENABLED=0 go build -o bin/switchboard-mcp ./cmd/switchboard-mcp
