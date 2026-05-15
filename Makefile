BINARY     := xyllo
CMD_PATH   := ./cmd/xyllo
BUILD_DIR  := bin
PROTO_DIR  := proto
PROTO_OUT  := proto/xyllov1

.PHONY: all build run test lint proto docker clean

all: build

## build: compile the binary into bin/
build:
	@mkdir -p $(BUILD_DIR)
	go build -trimpath -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY) $(CMD_PATH)

## run: build and run locally on port 8080
run: build
	./$(BUILD_DIR)/$(BINARY) --port 8080

## test: run unit tests
test:
	go test ./... -race -cover

## test-integration: run integration tests (requires -tags integration)
test-integration:
	go test ./tests/... -tags integration -v

## lint: run golangci-lint (must be installed separately)
lint:
	golangci-lint run ./...

## proto: regenerate Go stubs from .proto files
proto:
	@mkdir -p $(PROTO_OUT)
	protoc \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto

## docker: build the Docker image
docker:
	docker build -t $(BINARY):latest .

## clean: remove build artefacts
clean:
	@rm -rf $(BUILD_DIR)
