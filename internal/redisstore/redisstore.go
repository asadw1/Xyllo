// Package redisstore provides a thin wrapper around go-redis/v9 that exposes
// only the Redis Streams operations used by Xyllo's StreamIngestor and the
// geo simulator. Keeping the surface minimal makes it straightforward to swap
// the underlying client or mock the store in tests.
package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yourusername/xyllo/config"
)

// busyGroupErr is the error string Redis returns when a consumer group already
// exists. Treated as a no-op so that restarting Xyllo doesn't fail.
const busyGroupErr = "BUSYGROUP Consumer Group name already exists"

// Client wraps a go-redis Client with stream-specific helpers.
type Client struct {
	rdb *redis.Client
}

// New dials Redis using cfg, validates the connection with PING, and returns a
// ready-to-use Client. Returns an error if the dial or PING fails.
func New(cfg config.RedisConfig) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redisstore: ping %q: %w", cfg.Addr, err)
	}

	return &Client{rdb: rdb}, nil
}

// XAdd appends data to stream as a single "data" field.
func (c *Client) XAdd(ctx context.Context, stream string, data []byte) error {
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"data": data},
	}).Err()
}

// CreateConsumerGroup creates the consumer group on stream starting from the
// beginning of the stream (ID "0"), creating the stream key if necessary via
// MKSTREAM. A BUSYGROUP error (group already exists) is silently ignored so
// that Xyllo can restart without manual cleanup.
func (c *Client) CreateConsumerGroup(ctx context.Context, stream, group string) error {
	err := c.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != busyGroupErr {
		return fmt.Errorf("redisstore: create group %q on %q: %w", group, stream, err)
	}
	return nil
}

// XReadGroup reads up to count undelivered messages from stream as consumer
// within group. It blocks for blockMs milliseconds when the stream is empty.
// Returns nil, nil on a clean timeout with no messages.
func (c *Client) XReadGroup(
	ctx context.Context,
	group, consumer, stream string,
	count, blockMs int64,
) ([]redis.XMessage, error) {
	result, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    time.Duration(blockMs) * time.Millisecond,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // clean timeout — no messages available
		}
		return nil, fmt.Errorf("redisstore: XREADGROUP on %q: %w", stream, err)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result[0].Messages, nil
}

// XAck acknowledges message id in group on stream.
func (c *Client) XAck(ctx context.Context, stream, group, id string) error {
	return c.rdb.XAck(ctx, stream, group, id).Err()
}

// Close closes the underlying Redis connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}
