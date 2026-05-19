# Xyllo — AI Agent Coding Standards

> These instructions apply to every AI coding agent working in this repository.
> Read this file in full before making any changes to the codebase.

---

## 1. Non-Negotiable Rules

1. **Never break existing functionality.** Before touching any file, understand
   what it does and how it connects to the rest of the pipeline. If a change
   carries any risk of regression, write or update tests first, confirm they
   pass on the existing code, then make the change and confirm they still pass.

2. **No silent drops.** This is a validation-first system. Errors must be
   surfaced — routed to the DLQ, returned as an error value, or propagated up
   the call stack. `_ = err` is never acceptable.

3. **Do not expand scope.** Implement exactly what was asked. Do not add
   features, refactor unrelated code, rename symbols, or introduce
   abstractions that were not requested.

4. **Preserve package boundaries.** The dependency graph is intentional.
   Packages must never import laterally across pipeline stages (e.g.,
   `batcher` must not import `ingestor`). Consult the Package Dependency
   Graph in `docs/architecture.md` before adding any import.

---

## 2. Architecture Awareness

Xyllo is a **multi-stage, backpressure-aware telemetry pipeline**. Every
package has a single, well-defined role:

| Package | Role | May import |
|---|---|---|
| `internal/ingestor` | Network boundary; HTTP & gRPC listeners | `config`, `dispatcher`, `translator`, `pool` |
| `internal/translator` | Anti-Corruption Layer; schema normalisation | `config` |
| `internal/dispatcher` | Worker pool; fan-out from buffered channel | `config`, `middleware`, `batcher`, `dlq`, `pool` |
| `internal/middleware` | Validation chain; schema + type checks | `config`, `translator` |
| `internal/auth` | API key & JWT validation | `config` |
| `internal/batcher` | Aggregation & upstream flush | `config`, `translator` |
| `internal/dlq` | Dead Letter Queue persistence | `config` |
| `internal/pool` | `sync.Pool`-backed payload allocator | (none) |
| `internal/metrics` | Prometheus instrumentation | `config` |
| `config` | Typed config structs & loader | (none) |

**Before adding a new import**, ask: does this dependency flow in the correct
direction? If unsure, re-read section 6 of `docs/architecture.md`.

---

## 3. Code Quality Standards

### 3.1 Clean Code

- Functions and methods do **one thing**. If a function needs a comment to
  explain _what_ it does (not _why_), it should be split.
- Maximum function length: **40 lines** of non-comment, non-blank code.
  Exceptions require an inline justification comment.
- No magic numbers or magic strings. Use named constants or config fields.
- Variable names must be descriptive at declaration site. Single-letter names
  are acceptable only for loop indices and short-lived `err` shadows.

### 3.2 SOLID Principles

| Principle | Xyllo application |
|---|---|
| **Single Responsibility** | Each type owns one pipeline concern. `Dispatcher` dispatches; it does not validate. |
| **Open / Closed** | Extend via interfaces (`middleware.Handler`, `InboundTranslator`, `DLQ` backends). Do not modify core types to accommodate new sources. |
| **Liskov Substitution** | Any `InboundTranslator` implementation must be substitutable for `Passthrough` without altering pipeline behaviour. |
| **Interface Segregation** | Prefer narrow interfaces (single-method where possible). Do not pass large concrete types when a focused interface suffices. |
| **Dependency Inversion** | High-level pipeline stages depend on interfaces, not concrete implementations. Wire concrete types only in `main.go`. |

### 3.3 Error Handling

- Always return errors to the caller. Use `fmt.Errorf("context: %w", err)` to
  wrap with context so the call chain is visible in logs.
- Distinguish between **operational errors** (invalid input, network timeout —
  normal, recoverable) and **programming errors** (nil pointer, index out of
  range — should never happen). Operational errors go to the DLQ or a 4xx
  response. Programming errors should `panic` with a descriptive message so
  they surface immediately in tests and development.
- Never swallow an error with a blank identifier or an empty `if err != nil {}`
  block.

### 3.4 Concurrency

- All shared mutable state must be protected. Prefer `sync.RWMutex` for
  read-heavy maps (e.g., the rate-limiter bucket map). Document the invariant
  the lock protects in a comment directly above the field declaration.
- Channel directions must be specified in function signatures:
  `send chan<- T`, `receive <-chan T`. Bidirectional channels are only
  acceptable inside the owning struct.
- Goroutines must be tracked in the owning struct's `sync.WaitGroup` so that
  `Shutdown()` can guarantee a clean drain. Never launch a goroutine without
  a corresponding mechanism to stop it.
- `sync.Pool` objects must be **reset before returning to the pool** and
  **not read after returning**. Treat `pool.Put()` as a memory free.

---

## 4. Comment & Documentation Standards

Xyllo uses **enterprise-grade GoDoc comments**. These are not optional.

### 4.1 Package comments

Every package must have a `// Package <name> ...` comment in its primary file
explaining its role in the pipeline, what it owns, and what it does not own.

```go
// Package dispatcher manages the bounded inbound channel and the worker pool
// that drains it. It is responsible for fan-out and failure routing only —
// it does not perform validation (see internal/middleware) or persistence
// (see internal/dlq).
package dispatcher
```

### 4.2 Exported type & function comments

All exported types, functions, methods, and constants must have a GoDoc comment
that answers three questions:
1. What does this do?
2. What are the important preconditions or invariants?
3. What does the caller need to know about thread-safety, ownership, or
   lifecycle?

```go
// Submit enqueues p for asynchronous processing by the worker pool.
// It is safe to call from multiple goroutines concurrently.
// Returns false — and does not block — when the internal buffer is at
// capacity; the caller must treat a false return as a backpressure signal
// and respond with HTTP 429 / gRPC RESOURCE_EXHAUSTED.
func (d *Dispatcher) Submit(p Payload) bool { ... }
```

### 4.3 Inline comments

Use inline `//` comments to explain **why** a non-obvious decision was made,
not what the code does. If the why is architectural (e.g., "this avoids a
heap allocation on the hot path"), cite the relevant architecture doc section.

```go
// Non-blocking select — intentional. Backpressure is the caller's
// responsibility (see architecture.md §3.3 Dispatcher & Worker Pool).
select {
case d.buf <- p:
    return true
default:
    return false
}
```

### 4.4 TODO comments

All `TODO` comments must follow this format so they are trackable:

```
// TODO(<topic>): <description of what needs to be done>
```

Example: `// TODO(ingestor): Wire Fiber router with /v1/ingest and /healthz endpoints.`

---

## 5. Testing Standards

### 5.1 Coverage requirement

Every new or modified package must have **full unit test coverage** of all
exported functions, methods, and error paths. "Full" means:
- Every returned error value is exercised by at least one test case.
- Every branch (including `default` cases in `select` / `switch`) is covered.
- Concurrency paths (goroutine launch, channel drain, shutdown) are tested
  using `t.Parallel()` and appropriate synchronisation.

### 5.2 Test file conventions

- Test files live alongside the code they test: `foo.go` → `foo_test.go`.
- Integration tests live in `tests/` and carry the `//go:build integration`
  build tag so they are excluded from `go test ./...` by default.
- Use table-driven tests (`[]struct{ name, input, expected }`) for functions
  with multiple input/output combinations.
- Use `t.Helper()` in shared assertion helpers to preserve accurate line
  numbers in failure output.

### 5.3 Test naming

Test functions must follow `Test<Type>_<Method>_<Scenario>`:

```
TestDispatcher_Submit_ReturnsFalseWhenBufferFull
TestRateLimiter_Middleware_AllowsRequestUnderLimit
TestRegistry_Translate_ReturnsErrForUnknownSource
```

### 5.4 No test pollution

- Tests must not rely on global state. Construct fresh instances in each test.
- Tests must not write to the filesystem, open network connections, or start
  goroutines that outlive the test unless explicitly cleaned up via
  `t.Cleanup()`.
- Use interfaces and dependency injection to substitute real I/O with
  in-memory fakes. Never use `monkey-patching` or `init()` side effects to
  swap behaviour in tests.

### 5.5 Running tests

```
# Unit tests only (fast, no external deps)
go test ./...

# Integration tests (requires Docker / Redis)
go test -tags integration ./tests/...

# Race detector (always run before opening a PR)
go test -race ./...
```

---

## 6. Go Style & Conventions

- Follow the official [Effective Go](https://go.dev/doc/effective_go) and the
  [Google Go Style Guide](https://google.github.io/styleguide/go/).
- Run `gofmt` and `goimports` before committing. The Makefile target `make fmt`
  handles both.
- Lint with `golangci-lint run` (`make lint`). Do not suppress linter warnings
  with `//nolint` without an explanatory comment.
- Use `context.Context` as the **first parameter** of any function that may
  block, do I/O, or need cancellation. Never store a `Context` in a struct.
- Avoid `init()` functions. Side effects at package init time make testing and
  reasoning about startup order difficult.
- Avoid package-level variables unless they are unexported constants or are
  explicitly guarded by a `sync.Once`.

---

## 7. Security Standards (OWASP-aware)

- **Input validation at the boundary.** Raw bytes from external sources must
  pass through the translator and middleware chain before touching any internal
  type. No shortcutting.
- **No secrets in source.** API keys, JWT secrets, TLS private keys, and Redis
  passwords must be injected via environment variables or a secrets manager.
  `config.yaml` must never contain credentials. Raise an error at startup if a
  required secret env var is empty.
- **Constant-time comparisons.** API key checks must use `subtle.ConstantTimeCompare`
  to prevent timing-based extraction attacks.
- **TLS everywhere in production.** The ingestor must refuse to start without
  a valid TLS certificate when `cfg.Env == "production"`.
- **DLQ entries must not log raw payloads at INFO level.** Raw bytes may
  contain PII. Log only the event ID, source, and rejection reason at INFO;
  the full payload goes to the DLQ storage backend only.

---

## 8. Change Safety Checklist

Before marking any task complete, confirm all of the following:

- [ ] `go build ./...` succeeds with zero errors and zero warnings.
- [ ] `go test -race ./...` passes with no failures and no data-race reports.
- [ ] `golangci-lint run` reports no new issues.
- [ ] All new exported symbols have GoDoc comments.
- [ ] No new cross-package import was added that violates the dependency graph.
- [ ] No secret, credential, or PII is present in source or test files.
- [ ] Every new error path is covered by at least one test case.
- [ ] `TODO` comments follow the `// TODO(<topic>): ...` format.
