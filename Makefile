BINARY_NAME=scylla-migrate
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT?=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE?=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-s -w -X github.com/scylla-migrate/scylla-migrate/cmd.version=$(VERSION) -X github.com/scylla-migrate/scylla-migrate/cmd.commit=$(COMMIT) -X github.com/scylla-migrate/scylla-migrate/cmd.date=$(DATE)"

.PHONY: all build install test test-coverage lint fmt clean docker-build docker-test release-dry-run

all: lint test build

build:
	go build $(LDFLAGS) -o $(BINARY_NAME) .

install:
	go install $(LDFLAGS) .

test:
	go test ./... -v -count=1

test-coverage:
	go test ./... -v -count=1 -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html

test-integration:
	go test ./... -v -count=1 -tags integration

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .
	goimports -w .

clean:
	rm -f $(BINARY_NAME)
	rm -f coverage.txt coverage.html

docker-build:
	docker build -t $(BINARY_NAME):$(VERSION) .

docker-test:
	docker-compose -f docker-compose.test.yml up -d
	sleep 30
	go test ./... -v -count=1 -tags integration
	docker-compose -f docker-compose.test.yml down

release-dry-run:
	goreleaser release --snapshot --clean
