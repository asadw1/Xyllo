# Xyllo — Architecture Reference

> **Audience:** Engineers contributing to or integrating with Xyllo.  
> **Scope:** Internal design, data flow, package responsibilities, concurrency model, security model, and observability.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [High-Level Data Flow](#2-high-level-data-flow)
3. [Pipeline Stages](#3-pipeline-stages)
   - 3.1 [Ingestor](#31-ingestor)
   - 3.2 [Translator / Anti-Corruption Layer](#32-translator--anti-corruption-layer)
   - 3.3 [Dispatcher & Worker Pool](#33-dispatcher--worker-pool)
   - 3.4 [Middleware Chain](#34-middleware-chain)
   - 3.5 [Batcher / Exporter](#35-batcher--exporter)
   - 3.6 [Dead Letter Queue (DLQ)](#36-dead-letter-queue-dlq)
4. [Cross-Cutting Concerns](#4-cross-cutting-concerns)
   - 4.1 [Authentication](#41-authentication)
   - 4.2 [Rate Limiting](#42-rate-limiting)
   - 4.3 [Memory Management — sync.Pool](#43-memory-management--syncpool)
   - 4.4 [Observability](#44-observability)
   - 4.5 [TLS / Transport Security](#45-tls--transport-security)
5. [Configuration Model](#5-configuration-model)
6. [Package Dependency Graph](#6-package-dependency-graph)
7. [Concurrency Model](#7-concurrency-model)
8. [Error Handling & Failure Modes](#8-error-handling--failure-modes)
9. [Wire-up: Startup Sequence](#9-wire-up-startup-sequence)
10. [Proto Contract](#10-proto-contract)
11. [Design Decisions & Trade-offs](#11-design-decisions--trade-offs)
12. [Planned: Redis Streams & Geo Simulation](#12-planned-redis-streams--geo-simulation)

---

## 1. System Overview

Xyllo is a **unified telemetry and event ingestion engine** written in Go. Its role is to act as a hardened front door for distributed telemetry — accepting raw events from many heterogeneous sources, validating them rigorously, and delivering only clean, structured data to upstream analytics systems.

The design is governed by three principles:

| Principle | Implementation |
|---|---|
| **Validation-first** | Every payload passes through the middleware chain before it is eligible for export. Failures are never silently dropped — they are routed to the DLQ. |
| **Backpressure-aware** | The buffered channel between the ingestor and worker pool acts as the single congestion point. When it is full, inbound requests receive an explicit rejection rather than causing unbounded memory growth. |
| **Zero-copy where possible** | `sync.Pool` reuses `Payload` allocations. The backing byte slice is retained across pool round-trips; only its length is reset. |

---

## 2. High-Level Data Flow

```
                        ┌─────────────────────────────────────────────────┐
                        │                     Ingestor                    │
  [ Client / Source ] ──►  POST /v1/ingest  ──►  Auth ──►  RateLimit     │
                        │                             │                   │
                        └─────────────────────────────┼───────────────────┘
                                                      │ pool.Get()
                                                      ▼
                        ┌─────────────────────────────────────────────────┐
                        │     Inbound ACL — Translator / Registry          │
                        │  Registry.Translate(source, raw) → *Event        │
                        │  source-specific schema ──► canonical Event      │
                        └──────────────────────┬──────────────────────────┘
                                               │ on error → HTTP 422
                                               ▼
                        ┌─────────────────────────────────────────────────┐
                        │           Buffered Channel  (chan Event)         │
                        │               capacity = cfg.BufferSize         │
                        └──────────────────────┬──────────────────────────┘
                                               │  N goroutines
                                               ▼
                        ┌─────────────────────────────────────────────────┐
                        │           Dispatcher / Worker Pool               │
                        │                                                 │
                        │   ┌──────────────────────────────────────────┐  │
                        │   │          Middleware Chain                 │  │
                        │   │  SchemaValidator ──► TypeChecker ──► ...  │  │
                        │   └───────────────┬──────────────────────────┘  │
                        │                  │                              │
                        │           ┌──────┴──────┐                       │
                        │         VALID         INVALID                   │
                        └────────────┼──────────────┼─────────────────────┘
                                     │              │
                                     ▼              ▼
                           ┌──────────────┐  ┌──────────────┐
                           │   Batcher    │  │     DLQ      │
                           │  (aggregate) │  │  (reject log)│
                           └──────┬───────┘  └──────────────┘
                                  │ OutboundTranslator.Marshal(event)
                                  │ flush (size or time trigger)
                                  ▼
                        [ Upstream Analytics System ]
```

---

## 3. Pipeline Stages

### 3.1 Ingestor

**Package:** `internal/ingestor`  
**Key type:** `Server`

The ingestor is the network boundary of Xyllo. It owns two listener surfaces:

| Surface | Library | Default Port |
|---|---|---|
| HTTP REST | Fiber (v2) | `8080` |
| gRPC | `google.golang.org/grpc` | `9090` |

**Responsibilities:**
- Parse and lightly deserialise the incoming request (headers, source ID).
- Obtain a `Payload` from `pool.Get()` to avoid heap allocations.
- Call `translator.Registry.Translate(source, raw)` to normalise the raw bytes into a canonical `Event`. Translation failures are rejected immediately with `HTTP 422 / gRPC INVALID_ARGUMENT`.
- Call `Dispatcher.Submit(event)`. If `Submit` returns `false` (buffer full), respond immediately with `HTTP 429 / gRPC RESOURCE_EXHAUSTED`.
- Expose `/healthz` (liveness) and `/readyz` (readiness) probes.
- Register the Prometheus `/metrics` handler on a separate port (default `9090`).

**HTTP endpoints (planned):**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/ingest` | Single-event ingestion |
| `POST` | `/v1/ingest/batch` | Batch ingestion |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/readyz` | Readiness probe |
| `GET` | `/metrics` | Prometheus scrape |

**gRPC services** (defined in `proto/event.proto`):

| RPC | Type | Purpose |
|---|---|---|
| `Ingest` | Unary | Single event |
| `IngestStream` | Client-streaming | High-throughput stream |

---

### 3.2 Translator / Anti-Corruption Layer

**Package:** `internal/translator`  
**Key types:** `Event`, `InboundTranslator`, `OutboundTranslator`, `Registry`, `Passthrough`, `JSONOutbound`

The Translator is Xyllo's Anti-Corruption Layer (ACL). It sits at both edges of the internal pipeline, isolating Xyllo's domain model from the schemas of external sources and upstream consumers.

#### Canonical Event

All internal pipeline stages — the dispatcher, middleware chain, and batcher — operate exclusively on `*translator.Event`. Raw source bytes never cross the ingestor boundary.

```go
type Event struct {
    ID          string          // UUID assigned at ingestion
    Source      string          // originating client
    EventType   string          // "click", "pageview", "metric", etc.
    TimestampMS int64           // normalised to Unix milliseconds (UTC)
    ReceivedAt  time.Time       // wall-clock ingest time
    Body        json.RawMessage // source-agnostic payload (raw JSON)
}
```

#### Inbound ACL (source → pipeline)

The `Registry` maps source identifiers to `InboundTranslator` implementations. At startup, each known source is registered with a translator that handles its specific schema. Unknown sources fall through to `Passthrough`, which performs minimal normalisation (field extraction + timestamp coercion).

```
[ Source A (custom schema)   ] ─► SourceATranslator.Translate() ─►
[ Source B (RFC 3339 ts)     ] ─► SourceBTranslator.Translate() ─►  *Event ─► chan Event
[ Unknown / generic source   ] ─► Passthrough.Translate()        ─►
```

#### Outbound ACL (pipeline → upstream)

The `OutboundTranslator` interface is called inside the batcher's flush operation to convert the internal `Event` into the wire format the upstream analytics system expects. The default `JSONOutbound` marshals the canonical struct as-is; replace it with a source-specific implementation (Avro, Parquet, custom schema) without touching any other pipeline code.

#### Why this boundary matters

Without an ACL, the middleware chain must understand every source dialect and the batcher must understand every upstream format. Both layers accumulate external schema knowledge over time — a textbook corruption of the internal model. The Translator confines that knowledge to a single package with a clean interface boundary that can be extended or replaced without touching the core pipeline.

---

### 3.3 Dispatcher & Worker Pool

**Package:** `internal/dispatcher`  
**Key type:** `Dispatcher`

The dispatcher decouples inbound I/O from validation processing using Go's native concurrency primitives.

```
Dispatcher
│
├── buf   chan Payload          ← bounded FIFO, capacity = cfg.BufferSize
├── wg    sync.WaitGroup        ← tracks live workers for clean shutdown
└── [cfg.Workers goroutines]
        └── process()
                ├── pull from buf
                ├── chain.Run(result)
                ├── on success → batcher.Add(data)
                └── on failure → dlq.Push(source, reason, raw)
                                 pool.Put(payload)
```

**Backpressure:** `Submit` uses a non-blocking `select`. When `buf` is at capacity the call returns `false` immediately, leaving flow control to the ingestor layer (which responds with a 429).

**Shutdown:** `Shutdown()` closes `buf`, causing all worker `range` loops to drain remaining items and then exit cleanly. `wg.Wait()` blocks until all workers have finished.

---

### 3.4 Middleware Chain

**Package:** `internal/middleware`  
**Key types:** `Handler`, `Chain`, `Result`, `RateLimiter`

The middleware chain is the "sieve" of the pipeline. It is a linear sequence of `Handler` functions, each with the signature:

```go
type Handler func(r *Result) error
```

A `Result` carries the payload data plus any accumulated metadata through the chain. Execution stops on the first error — the payload is not passed to subsequent handlers.

**Built-in handlers (planned order):**

| # | Handler | Package | Purpose |
|---|---|---|---|
| 1 | `auth.APIKeyValidator` / `auth.JWTValidator` | `internal/auth` | Reject unauthenticated sources |
| 2 | `RateLimiter.Middleware` | `internal/middleware` | Enforce per-source token bucket |
| 3 | `SchemaValidator` | `internal/middleware` | Structural JSON schema check |
| 4 | `TypeChecker` | `internal/middleware` | Per-field type assertion |

The chain is constructed once at startup in `main.go` and shared (read-only) across all worker goroutines — no locks required.

**Adding a new handler** requires only implementing the `Handler` signature and passing it to `middleware.NewChain(...)`. No interface registration, no reflection.

---

### 3.5 Batcher / Exporter

**Package:** `internal/batcher`  
**Key type:** `Batcher`

The batcher accumulates validated payloads and delivers them upstream in efficient bulk writes rather than one event at a time.

**Flush triggers (either condition):**

| Trigger | Controlled by |
|---|---|
| Buffer reaches `cfg.MaxSize` events | `Batcher.Add()` |
| `cfg.FlushInterval` elapses | Internal `time.Ticker` goroutine |

The flush operation serialises the accumulated batch and sends it to the configured upstream endpoint. The internal buffer is swapped out under a `sync.Mutex` so that new events can continue to be added while the flush I/O is in-flight.

---

### 3.6 Dead Letter Queue (DLQ)

**Package:** `internal/dlq`  
**Key types:** `DLQ`, `Entry`

Any payload rejected by the middleware chain is written to the DLQ via `DLQ.Push(source, reason, raw)`. Each entry is stamped with a UTC timestamp and serialised as JSON.

**Pluggable backends** (driven by `cfg.DLQ.Backend`):

| Backend value | Target format | Notes |
|---|---|---|
| `file` | File path | Line-delimited JSON; default for development |
| `redis` | Redis DSN | Written via `XADD` to a Redis Stream |
| `kafka` | Broker list | Produced to a configured topic |

DLQ writes are best-effort fire-and-forget from the worker's perspective — a DLQ write failure must not cause the worker to stall or panic.

---

## 4. Cross-Cutting Concerns

### 4.1 Authentication

**Package:** `internal/auth`

Auth is implemented as middleware `Handler` values, making it composable with any other chain step. The active mode is selected by `cfg.Auth.Mode`:

| Mode | Mechanism | Handler |
|---|---|---|
| `none` | No-op (dev/internal) | — |
| `apikey` | `X-API-Key` header, constant-time comparison | `APIKeyValidator` |
| `jwt` | `Authorization: Bearer <token>`, HMAC-SHA256 | `JWTValidator` |

Auth handlers run first in the chain so that downstream validation work is never performed on unauthenticated requests.

**Security note:** API keys and JWT secrets must never be placed in `config.yaml` committed to version control. Use environment variable overrides or a secrets manager to inject them at runtime.

---

### 4.2 Rate Limiting

**Package:** `internal/middleware` (`ratelimit.go`)  
**Algorithm:** Token Bucket

Each unique `Source` value receives its own independent bucket. Buckets are created lazily on first request and are stored in a `sync.Mutex`-protected map.

```
bucket (per source)
├── tokens   float64    ← current token count
├── capacity float64    ← = cfg.BurstSize
├── rate     float64    ← tokens/second = cfg.RequestsPerSecond
└── lastFill time.Time  ← used to compute elapsed refill
```

On each request, elapsed time since `lastFill` is used to add `elapsed × rate` tokens (capped at `capacity`). If at least one token is available it is consumed and the request proceeds; otherwise the handler returns an error that causes an immediate 429 response.

**Defaults:** 1 000 RPS steady-state, 2 000-event burst per source.

---

### 4.3 Memory Management — sync.Pool

**Package:** `internal/pool`

All `Payload` objects are obtained from and returned to a `sync.Pool`. The pool's `New` function pre-allocates a 4 KB backing byte slice, covering the vast majority of telemetry payloads without reallocation.

**Lifecycle:**

```
Ingestor receives request
    └── pool.Get()          ← obtain (or allocate) Payload
    └── copy data into Payload.Data
    └── Dispatcher.Submit(payload)
            └── worker processes payload
            └── pool.Put(payload)   ← reset and return to pool
```

`pool.Put` resets `Source` to `""` and truncates `Data` to length zero while retaining the backing array, avoiding GC pressure during high-throughput bursts.

---

### 4.4 Observability

**Package:** `internal/metrics`  
**Library:** `prometheus/client_golang`

All metrics are registered at package initialisation via `promauto` and are globally accessible. Packages increment the relevant metric at the point of observation.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `xyllo_events_ingested_total` | Counter | `source` | Events accepted by the ingestor |
| `xyllo_events_rejected_total` | Counter | `source`, `reason` | Events rejected by the middleware chain |
| `xyllo_dlq_enqueued_total` | Counter | — | Payloads written to the DLQ |
| `xyllo_batcher_flushes_total` | Counter | — | Upstream batch flush operations |
| `xyllo_worker_pool_buffer_depth` | Gauge | — | Current dispatcher channel depth |
| `xyllo_rate_limited_requests_total` | Counter | `source` | Requests dropped by the rate limiter |

The `/metrics` endpoint is served by `metrics.Handler()` and is intended to be scraped by Prometheus on a dedicated port to keep it off the public ingestion surface.

---

### 4.5 TLS / Transport Security

TLS is controlled by `cfg.TLS`. When `enabled: true`, the ingestor loads the PEM certificate and private key from `cert_file` / `key_file` and configures both the Fiber HTTP server and the gRPC server to use them.

In plain-text mode (`enabled: false`) the service binds without TLS — appropriate for environments where TLS termination is handled by a sidecar or load balancer.

---

## 5. Configuration Model

Configuration is loaded by `config.Load(path)` from a YAML file. The full struct hierarchy maps directly to `config.yaml`:

```
Config
├── DispatcherConfig   workers, buffer_size
├── BatcherConfig      max_size, flush_interval
├── DLQConfig          backend, target
├── ObservabilityConfig  metrics_path
├── TLSConfig          enabled, cert_file, key_file
├── AuthConfig         mode, api_key, jwt_secret, jwt_issuer
└── RateLimitConfig    enabled, requests_per_second, burst_size
```

**Planned:** environment variable overrides, with CLI flags taking final precedence. Order: defaults < YAML file < env vars < CLI flags.

---

## 6. Package Dependency Graph

Arrows indicate import direction (`A → B` means A imports B).

```
cmd/xyllo
    ├── config
    ├── internal/ingestor    → config, internal/dispatcher, internal/translator
    ├── internal/translator  (no internal imports — ACL leaf package)
    ├── internal/dispatcher  → config, internal/middleware,
    │                          internal/batcher, internal/dlq
    ├── internal/middleware  → config
    ├── internal/auth        → config, internal/middleware
    ├── internal/batcher     → config, internal/translator
    ├── internal/dlq         → config
    ├── internal/pool        (no internal imports)
    └── internal/metrics     (no internal imports — prometheus only)
```

`internal/translator`, `internal/pool`, and `internal/metrics` are leaf packages with no internal dependencies, making them safe to import from any layer without creating cycles.

---

## 7. Concurrency Model

```
Goroutine inventory at steady state
─────────────────────────────────────────────────────────────────────────────
1                  main goroutine         signal/ctx management
1                  Fiber HTTP server      managed by Fiber internally
1                  gRPC server            managed by grpc.Server
cfg.Workers        dispatcher workers     range over buf chan
1                  batcher flush ticker   periodic time.Ticker
─────────────────────────────────────────────────────────────────────────────
Total: cfg.Workers + 4 goroutines at minimum
```

**Shared state and its protection:**

| Shared state | Protected by |
|---|---|
| `Dispatcher.buf` (channel) | Inherent channel synchronisation |
| `Batcher.buf` (slice) | `sync.Mutex` |
| `RateLimiter.buckets` (map) | `sync.Mutex` |
| `bucket.tokens` / `lastFill` | Per-bucket `sync.Mutex` |
| `pool.payloadPool` | `sync.Pool` internals |
| Prometheus metrics | `prometheus/client_golang` internals |

There are no global variables mutated after startup except within the packages listed above, each of which is internally synchronised.

---

## 8. Error Handling & Failure Modes

| Failure | Behaviour | Recovery |
|---|---|---|
| Inbound buffer full | `Dispatcher.Submit` returns `false`; ingestor returns 429 / `RESOURCE_EXHAUSTED` | Client retries with backoff |
| Translation failure (malformed/unknown schema) | `Registry.Translate` returns error; ingestor returns 422 / `INVALID_ARGUMENT`; payload never enters the channel | Source owner fixes schema; no DLQ entry (payload was not a valid event) |
| Middleware validation failure | Worker routes payload to DLQ via `dlq.Push`; increments `xyllo_events_rejected_total` | Operator inspects DLQ entries |
| DLQ write failure | Logged; payload is discarded (best-effort) | Operator checks DLQ backend availability |
| Batcher flush failure | Logged; batch is discarded (retry strategy TBD) | Implement retry with exponential backoff |
| Worker panic | Should be caught by a `recover()` wrapper in `process()` (TODO) | Worker goroutine is restarted |
| Config load failure | `log.Fatalf` — process exits immediately | Operator fixes config and restarts |

---

## 9. Wire-up: Startup Sequence

```
main()
 │
 ├─ config.Load(path)                   // load & validate config
 │
 ├─ translator.NewRegistry()            // create ACL registry with Passthrough fallback
 │      .Register("source-a", ...)     // register source-specific translators
 │
 ├─ dlq.New(cfg.DLQ)                    // initialise DLQ sink
 │
 ├─ middleware.NewChain(                 // build validation chain
 │      auth.APIKeyValidator(cfg.Auth), //   (when mode != "none")
 │      rl.Middleware(),                //   rate limiter
 │      middleware.SchemaValidator(),   //   schema check
 │      middleware.TypeChecker(),       //   type check
 │  )
 │
 ├─ batcher.New(cfg.Batcher)            // start flush ticker goroutine
 │
 ├─ dispatcher.New(cfg, chain, bat, dlq) // allocate channel + start workers
 │
 ├─ ingestor.New(port, cfg, disp, reg)  // construct server (not yet listening)
 │
 ├─ signal.NotifyContext(...)           // arm SIGINT / SIGTERM handler
 │
 └─ srv.Start(ctx)                      // bind listeners; block until ctx done
         └─ graceful shutdown on ctx cancel
                 ├─ stop accepting new requests
                 ├─ disp.Shutdown()     // drain buffer, wait for workers
                 └─ batcher.Stop()      // final flush + stop ticker
```

---

## 10. Proto Contract

**File:** `proto/event.proto`  
**Go package:** `github.com/yourusername/xyllo/proto/xyllov1`

```protobuf
message EventRequest {
  string source            = 1;  // originating service / device
  string event_type        = 2;  // "click", "pageview", "metric", etc.
  int64  timestamp_unix_ms = 3;  // UTC milliseconds
  bytes  payload           = 4;  // raw JSON body
}

message EventResponse {
  bool   accepted      = 1;
  string error_message = 2;  // populated only when accepted == false
}

service IngestorService {
  rpc Ingest(EventRequest)              returns (EventResponse);
  rpc IngestStream(stream EventRequest) returns (stream EventResponse);
}
```

Stubs are generated via `make proto`. The generated files are committed to `proto/xyllov1/` and must not be hand-edited.

---

## 11. Design Decisions & Trade-offs

| Decision | Rationale | Trade-off |
|---|---|---|
| **Single buffered channel as the pipeline join point** | Simple, lock-free handoff between ingestor and workers; backpressure is automatic. | Single point of congestion — sizing `buffer_size` correctly is critical. |
| **Middleware as plain functions (`Handler`)** | Zero reflection, trivially composable, easy to test in isolation. | No built-in retry or conditional branching within the chain; must be handled by individual handlers. |
| **`sync.Pool` for payload allocation** | Dramatically reduces GC pressure at high RPS by reusing 4 KB backing arrays. | Payloads must be explicitly returned via `pool.Put`. Forgetting to call `Put` after an error path causes a slow allocation leak. |
| **Token Bucket over Leaky Bucket** | Allows controlled bursting (up to `burst_size`) while still enforcing a steady-state ceiling, which better matches real telemetry traffic patterns. | Per-source map grows unbounded over time; a TTL eviction strategy is needed for long-running deployments with many ephemeral sources. |
| **DLQ is best-effort** | Avoids back-pressure from a slow DLQ backend stalling the worker pool. | In a failure storm, DLQ entries may be lost if the backend is unavailable. A durable queue (Kafka/Redis) mitigates this. |
| **Anti-Corruption Layer via `internal/translator`** | Confines all external schema knowledge to one package. The middleware chain and batcher never need to know about source dialects or upstream formats. Adding a new source is a one-file change. | Requires an explicit `Register` call per source at startup. Sources not registered fall through to `Passthrough`, which may silently accept structurally valid but semantically wrong payloads if the schema differs subtly. |
| **Distroless runtime image** | Minimal attack surface; no shell, no package manager. | Harder to debug live containers — use `kubectl debug` with an ephemeral container. |
| **Redis Streams as multi-region event bus (planned)** | Decouples the simulator and any real remote producers from Xyllo's HTTP/gRPC surface. Each region gets its own stream; consumer groups let multiple Xyllo replicas share the load without duplicate processing. | Adds an operational dependency (Redis). Per-region streams require the consumer to know the full region list at startup; dynamic stream discovery needs a separate registry or naming convention. |

---

## 12. Planned: Redis Streams & Geo Simulation

> **Status:** Planned — Phase 4. No code has been written yet.

### Overview

This phase introduces a second ingest path alongside HTTP/gRPC. A standalone binary (`cmd/simulator`) publishes synthetic, geo-tagged telemetry events to per-region Redis Streams. A new `StreamIngestor` inside the Xyllo server reads from those streams using Redis consumer groups and feeds events through the existing translator → dispatcher → middleware → batcher pipeline.

This produces a self-contained, reproducible demo of multi-region ingestion without requiring real client integrations.

### Updated Data Flow

```
[ cmd/simulator ]
  ├─ XADD xyllo:stream:us-east-1    ┐
  ├─ XADD xyllo:stream:eu-west-1    │
  ├─ XADD xyllo:stream:ap-southeast-1│  Redis
  ├─ XADD xyllo:stream:sa-east-1    │  Streams
  └─ XADD xyllo:stream:af-south-1   ┘
                                       │
          XREADGROUP (one goroutine per region)
                                       │
                          ▼
                 [ StreamIngestor ]
                 reg.Translate(region, raw)
                 disp.Submit(event)
                 XACK msg.ID
                          │
                  (joins existing pipeline)
                          ▼
             Dispatcher → Middleware → Batcher → Upstream
```

### New Packages

| Package | Responsibility |
|---|---|
| `internal/redisstore` | Thin wrapper around `go-redis/v9`. Manages the client connection, exposes `XAdd`, `EnsureGroup`, `ReadGroup`, and `Ack`. No pipeline logic. |
| `internal/streamingestor` | One goroutine per configured region. Calls `ReadGroup` in a loop, translates each message via the ACL registry, submits to the dispatcher, then ACKs. Stops cleanly when `ctx` is cancelled. |
| `cmd/simulator` | Standalone binary. Configurable regions, event rate per region, and stream prefix. Generates events with randomised geo fields (lat, lng, city, country) and event types (`pageview`, `click`, `metric`, `error`, `transaction`). Publishes via `XADD` with `MAXLEN ~ 100000` to cap stream size. |

### Redis Stream Key Design

```
xyllo:stream:<region>
```

Examples: `xyllo:stream:us-east-1`, `xyllo:stream:eu-west-1`.

The prefix (`xyllo:stream`) and region list are both configurable via `RedisConfig`. Consumer group name defaults to `xyllo-workers`; the consumer identity is `xyllo-<hostname>-<pid>` to avoid collisions across replicas.

### Message Schema (Redis Stream fields)

| Field | Type | Description |
|---|---|---|
| `source` | string | Region identifier, e.g. `us-east-1` |
| `event_type` | string | `pageview`, `click`, `metric`, `error`, `transaction` |
| `payload` | JSON string | Full event body including geo fields (lat, lng, city, country) and timestamp |

The `payload` field is passed verbatim to `translator.Registry.Translate(source, payload)`. The default `Passthrough` translator handles it; source-specific translators can be registered for any region that sends a non-canonical schema.

### New Config Section (`RedisConfig`)

```yaml
redis:
  addr: "localhost:6379"
  password: ""
  db: 0
  stream_prefix: "xyllo:stream"
  consumer_group: "xyllo-workers"
  streams:
    - us-east-1
    - eu-west-1
    - ap-southeast-1
    - sa-east-1
    - af-south-1
```

### Updated Package Dependency Graph (after Phase 4)

```
cmd/xyllo
    ├── internal/streamingestor  → internal/redisstore, internal/translator,
    │                              internal/dispatcher
    ├── internal/redisstore      (no internal imports — go-redis leaf)
cmd/simulator
    └── internal/redisstore
```

All existing package relationships from §6 remain unchanged.

### Geo Regions & Bounding Boxes

| Region | Approx. bounding box | Example cities |
|---|---|---|
| `us-east-1` | lat 37–42, lng −80 to −71 | New York, Boston, Washington DC |
| `eu-west-1` | lat 47–55, lng −10 to 15 | London, Paris, Dublin, Amsterdam |
| `ap-southeast-1` | lat 1–14, lng 100–121 | Singapore, Bangkok, Kuala Lumpur |
| `sa-east-1` | lat −35 to −3, lng −70 to −35 | São Paulo, Buenos Aires, Santiago |
| `af-south-1` | lat −35 to 5, lng 15–45 | Cape Town, Johannesburg, Nairobi |
