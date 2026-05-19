# Xyllo

**Unified Telemetry & Event Ingestion Engine**

Xyllo is a lightweight, high-performance ingestion service written in Go. It is designed to act as a standardized entry point for distributed telemetry, providing high-concurrency transit with a "validation-first" philosophy to protect upstream analytics systems from data corruption.

## Core Features

- **High Concurrency:** Built on Go's worker pool pattern for massive throughput with minimal overhead.
- **Zero-Copy Validation:** Leverages `sync.Pool` and fast-path parsing to maintain a small memory footprint.
- **Standardized Middleware:** Pluggable data validation layers to ensure schema integrity.
- **Backpressure Aware:** Built-in safeguards to handle traffic spikes without system failure.
- **Dead Letter Queue (DLQ):** Automatic routing of malformed payloads for offline inspection.

## Architecture Overview

Xyllo operates as a multi-stage pipeline:

1. **Ingestor:** Fast HTTP/gRPC handlers that accept and buffer raw data.
2. **Dispatcher:** Manages worker goroutines to process the buffer.
3. **Validator Middleware:** Executes structural and semantic checks.
4. **Batcher/Exporter:** Aggregates clean data for efficient upstream delivery.

### Data Flow Diagram

```text
[ Source ] -> [ Ingestor (HTTP/gRPC) ] -> [ Buffered Channel ]
                                                 |
                                         [ Dispatcher/Worker Pool ]
                                                 |
                                      [ Validator Middleware ] --(Invalid)--> [ DLQ ]
                                                 |
                                             (Valid)
                                                 |
                                        [ Aggregator/Batcher ] -> [ Upstream Analytics ]
```

## Project Structure

```
Xyllo/
├── cmd/
│   └── xyllo/             # Application entrypoint (main.go)
├── internal/
│   ├── ingestor/          # HTTP & gRPC handlers
│   ├── translator/        # Anti-Corruption Layer — canonical Event + source/upstream translators
│   ├── dispatcher/        # Worker pool & buffer management
│   ├── middleware/        # Validation middleware chain + rate limiter
│   ├── auth/              # API Key & JWT authentication middleware
│   ├── pool/              # sync.Pool-backed payload allocator (zero-copy)
│   ├── metrics/           # Prometheus counters, gauges, and /metrics handler
│   ├── dlq/               # Dead Letter Queue logic
│   └── batcher/           # Aggregation & upstream export
├── config/
│   ├── config.go          # Typed config structs + Load()
│   └── config.yaml        # Default runtime values
├── proto/                 # Protobuf definitions (event.proto)
├── tests/                 # Integration & load tests
├── Dockerfile
├── Makefile
├── go.mod
└── README.md
```

## Roadmap & Timeline

The development of Xyllo is divided into five distinct phases, moving from a functional prototype to a production-ready showcase.

### Phase 1: Core Engine & Pipeline

**Timeline:** Weeks 1-3 | **Focus:** Fundamental I/O and Concurrency

**Milestones:**
- [x] Initialize Go module, package structure, and confirm clean build.
- [x] Wire up HTTP (Fiber) and gRPC listeners in the ingestor.
- [x] Launch worker goroutines in the dispatcher and connect to the buffered channel.
- [x] Design `sync.Pool` memory management for payload reuse (`internal/pool`).

**Deliverable:** A functional engine that can receive, log, and acknowledge events.

---

### Phase 2: Middleware & Data Integrity

**Timeline:** Weeks 4-5 | **Focus:** The "Sieve" logic and error handling

**Milestones:**
- [x] Develop the standardized middleware interface (`SchemaValidator`, `TypeChecker`, pluggable `Chain`).
- [x] Implement JSON schema validation and type checking (middleware package, 29 tests).
- [x] Build the Dead Letter Queue (DLQ) for rejected payloads (file backend, concurrent-safe, 12 tests).
- [x] Add typed error handling (`internal/xlerr` — `PipelineError` with stage/code/source).
- [x] Add panic recovery in dispatcher workers (stack trace logged, worker continues, metric emitted).
- [x] YAML config loading with env-variable overrides and validation (`config.Load`, 23 tests).

**Deliverable:** An engine that drops malformed data and passes clean data to the sink.

---

### Phase 3: Auth & Security Hardening

**Timeline:** Weeks 6-7 | **Focus:** Access control and transit security

**Milestones:**
- [ ] Implement API Key / JWT authentication middleware.
- [ ] Add TLS support for secure telemetry transit.
- [ ] Integrate rate limiting (Token Bucket) per source.

**Deliverable:** A secure gateway that restricts ingestion to authorized clients.

---

### Phase 4: Redis Streams & Geo Simulation

**Timeline:** Weeks 8-9 | **Focus:** Multi-region event sourcing and realistic load generation

**Milestones:**
- [ ] Add `internal/redisstore` — thin `go-redis/v9` client wrapper (XADD, XREADGROUP, XACK, consumer group management).
- [ ] Add `internal/streamingestor` — Redis Streams consumer that reads from per-region streams, translates via the ACL registry, and feeds the dispatcher.
- [ ] Add `cmd/simulator` — geo event generator that publishes synthetic telemetry to `xyllo:stream:<region>` for five configurable regions (`us-east-1`, `eu-west-1`, `ap-southeast-1`, `sa-east-1`, `af-south-1`).
- [ ] Extend `config.RedisConfig` and `config/config.yaml` with addr, stream prefix, consumer group, and region list.
- [ ] Wire `StreamIngestor` into the startup sequence alongside the HTTP/gRPC ingestor.
- [ ] Add a `docker-compose.yml` with Redis, Xyllo server, and simulator services.
- [ ] Add Prometheus and Grafana to `docker-compose.yml` — Prometheus scrapes Xyllo's `:9091/metrics` endpoint; Grafana is pre-provisioned with a dashboard visualising ingestion rate, worker pool depth, DLQ depth, and events rejected. The simulator drives the traffic that populates all panels.

**Deliverable:** A self-contained demo where the simulator floods five regional streams and Xyllo ingests, translates, and dispatches all events end-to-end, with live metrics visible in Grafana.

---

### Phase 5: Load Testing & Optimization

**Timeline:** Weeks 8-9 | **Focus:** Breaking points and performance tuning

**Milestones:**
- [ ] Simulate high-load scenarios (10k-100k RPS) using k6 or Locust.
- [ ] Profile memory and CPU usage via ``pprof``.
- [ ] Optimize GC pressure and refine worker pool sizing.

**Deliverable:** Performance benchmarks and a stable build.

---

### Phase 6: Showcase & Documentation

**Timeline:** Week 10 | **Focus:** External communication and demonstration

**Milestones:**
- [ ] Finalize technical documentation and API specifications.
- [ ] Record a technical walkthrough/demo of the engine under load.
- [ ] Publish demonstration video and performance report.

**Deliverable:** Project launch and published demo.

---

## Tech Stack

| Component     | Technology               |
|---------------|--------------------------|
| Language      | Go (Golang)              |
| API           | Fiber / gRPC             |
| JSON Parsing  | Sonic or Fastjson        |
| Validation    | go-playground/validator  |
| Load Testing  | k6, pprof                |
| Observability | Prometheus               |

## Getting Started

### Prerequisites

| Requirement | Version | Install |
|---|---|---|
| Go | 1.22+ | https://go.dev/dl |
| make | any | `winget install GnuWin32.Make` (Windows) / pre-installed on macOS & Linux |
| protoc | 3.x+ | https://grpc.io/docs/protoc-installation |
| gocov | latest | `go install github.com/axw/gocov/gocov@v1.1.0` |
| gocov-html | latest | `go install github.com/matm/gocov-html/cmd/gocov-html@latest` |

After installing `make` on Windows, add it to your PATH:
```powershell
$env:PATH += ";C:\Program Files (x86)\GnuWin32\bin"
[Environment]::SetEnvironmentVariable("PATH", $env:PATH + ";C:\Program Files (x86)\GnuWin32\bin", "User")
```

After installing Go tools, ensure `$GOPATH/bin` is on your PATH:
```powershell
$env:PATH += ";$env:USERPROFILE\go\bin"
[Environment]::SetEnvironmentVariable("PATH", $env:PATH + ";$env:USERPROFILE\go\bin", "User")
```

### Install & Run

```bash
# Clone the repository
git clone https://github.com/yourusername/Xyllo.git
cd Xyllo

# Build the binary
go build -o Xyllo main.go

# Run the engine
./Xyllo --port 8080
```

## Running Tests

```bash
# Run all unit tests
make test

# Run integration tests (requires Docker)
make test-integration
```

## Coverage Report

Coverage is scoped to `internal/` — the composition root (`cmd/`) is excluded as it contains no testable logic.

```bash
# Generate coverage report: prints per-function table to terminal and opens
# the annotated HTML report in your browser
make cover
```

For a sortable single-page HTML table (requires `gocov` and `gocov-html`):

```bash
gocov test ./... | gocov-html > coverage-report.html
open coverage-report.html      # macOS
start coverage-report.html     # Windows
```

To view inline gutters in VS Code (green/red line coverage per file):
1. Install the [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) extension.
2. Run `make cover` to generate `coverage.out`.
3. Open any `.go` file and click **Watch** in the VS Code status bar.

## Configuration

Xyllo can be configured via environment variables or CLI flags. CLI flags take precedence.

| Flag / Env Var                  | Default     | Description                              |
|---------------------------------|-------------|------------------------------------------|
| `--port` / `PORT`               | `8080`      | HTTP listener port                       |
| `--grpc-port` / `GRPC_PORT`     | `9090`      | gRPC listener port                       |
| `--workers` / `WORKERS`         | `100`       | Worker pool size                         |
| `--buffer-size` / `BUFFER_SIZE` | `10000`     | Internal channel buffer capacity         |
| `--dlq-path` / `DLQ_PATH`       | `./dlq`     | Directory to write DLQ payloads          |
| `--tls-cert` / `TLS_CERT`       | _(none)_    | Path to TLS certificate file             |
| `--tls-key` / `TLS_KEY`         | _(none)_    | Path to TLS private key file             |
| `--metrics-port` / `METRICS_PORT` | `2112`    | Prometheus metrics exposition port       |

## API Reference

> Full API specification will be published in Phase 5. The following are the planned endpoints.

### HTTP

| Method | Endpoint         | Description                        |
|--------|------------------|------------------------------------|
| `POST` | `/v1/ingest`     | Submit a single event payload      |
| `POST` | `/v1/ingest/batch` | Submit a batch of event payloads |
| `GET`  | `/healthz`       | Liveness probe                     |
| `GET`  | `/readyz`        | Readiness probe                    |
| `GET`  | `/metrics`       | Prometheus metrics (default :2112) |

### gRPC

Service definition lives in `proto/ingestor.proto`.

| RPC             | Request            | Response           | Description                   |
|-----------------|--------------------|--------------------|-------------------------------|
| `Ingest`        | `EventRequest`     | `EventResponse`    | Submit a single event         |
| `IngestStream`  | `stream EventRequest` | `EventResponse` | Client-streaming ingestion    |

## Contributing

Contributions are welcome. Please open an issue to discuss proposed changes before submitting a pull request.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
