# ── Build stage ─────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Cache dependency downloads separately from source compilation.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /xyllo ./cmd/xyllo

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /xyllo /xyllo
COPY config/config.yaml /config/config.yaml

EXPOSE 8080
EXPOSE 9090

ENTRYPOINT ["/xyllo", "--port", "8080", "--config", "/config/config.yaml"]
