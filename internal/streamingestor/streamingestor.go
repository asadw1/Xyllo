// Package streamingestor reads events from per-region Redis Streams and feeds
// translated, canonical payloads into the dispatcher's bounded buffer.
//
// Architecture: one goroutine per configured region performs a blocking
// XREADGROUP loop. Undeliverable payloads (translation errors) are ACKed and
// counted as rejected. Payloads that cannot be submitted because the dispatcher
// buffer is full are NOT ACKed — Redis will redeliver them once the consumer
// catches up, providing automatic backpressure without data loss.
package streamingestor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/dispatcher"
	"github.com/yourusername/xyllo/internal/metrics"
	"github.com/yourusername/xyllo/internal/redisstore"
	"github.com/yourusername/xyllo/internal/translator"
)

// StreamIngestor reads from Redis Streams and submits payloads to the pipeline.
type StreamIngestor struct {
	cfg    config.RedisConfig
	client *redisstore.Client
	disp   *dispatcher.Dispatcher
	reg    *translator.Registry
}

// New creates a StreamIngestor. Call Start to begin consuming.
func New(
	cfg config.RedisConfig,
	client *redisstore.Client,
	disp *dispatcher.Dispatcher,
	reg *translator.Registry,
) *StreamIngestor {
	return &StreamIngestor{cfg: cfg, client: client, disp: disp, reg: reg}
}

// Start creates consumer groups for all configured regions, then launches one
// reader goroutine per region. It blocks until ctx is cancelled, at which point
// it waits for all goroutines to drain and exit cleanly.
func (s *StreamIngestor) Start(ctx context.Context) error {
	for _, region := range s.cfg.Regions {
		stream := s.streamKey(region)
		if err := s.client.CreateConsumerGroup(ctx, stream, s.cfg.ConsumerGroup); err != nil {
			return fmt.Errorf("streamingestor: setup region %q: %w", region, err)
		}
	}

	var wg sync.WaitGroup
	for _, region := range s.cfg.Regions {
		region := region
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.consumeRegion(ctx, region)
		}()
	}
	wg.Wait()
	return nil
}

// consumeRegion is the inner XREADGROUP loop for a single region. It runs until
// ctx is cancelled.
func (s *StreamIngestor) consumeRegion(ctx context.Context, region string) {
	stream := s.streamKey(region)
	consumer := fmt.Sprintf("%s-%s", s.cfg.ConsumerName, region)
	log.Printf("[streamingestor] consumer %q started on stream %q", consumer, stream)

	for {
		// Check for cancellation before each blocking read.
		if ctx.Err() != nil {
			log.Printf("[streamingestor] consumer %q shutting down", consumer)
			return
		}

		msgs, err := s.client.XReadGroup(
			ctx,
			s.cfg.ConsumerGroup, consumer, stream,
			s.cfg.ReadBatchSize, s.cfg.ReadBlockMs,
		)
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled during blocking read
			}
			log.Printf("[streamingestor] XREADGROUP error on %q: %v", stream, err)
			continue
		}

		for _, msg := range msgs {
			s.handleMessage(ctx, stream, region, msg.ID, msg.Values)
		}
	}
}

// handleMessage translates a single Redis Stream message and submits it to the
// dispatcher. ACKs the message on success or unrecoverable error; withholds ACK
// on backpressure so Redis redelivers it.
func (s *StreamIngestor) handleMessage(
	ctx context.Context,
	stream, region, msgID string,
	values map[string]any,
) {
	raw, ok := extractBytes(values["data"])
	if !ok {
		log.Printf("[streamingestor] msg %q has no usable 'data' field — skipping", msgID)
		_ = s.client.XAck(ctx, stream, s.cfg.ConsumerGroup, msgID)
		return
	}

	event, err := s.reg.Translate(region, raw)
	if err != nil {
		log.Printf("[streamingestor] translate error msg %q: %v", msgID, err)
		metrics.RecordEventRejected(region, "translation_error")
		_ = s.client.XAck(ctx, stream, s.cfg.ConsumerGroup, msgID)
		return
	}

	if event.ID == "" {
		event.ID = uuid.NewString()
	}

	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[streamingestor] marshal error msg %q: %v", msgID, err)
		_ = s.client.XAck(ctx, stream, s.cfg.ConsumerGroup, msgID)
		return
	}

	if !s.disp.Submit(dispatcher.Payload{Source: region, Data: data}) {
		// Dispatcher buffer is full — withhold ACK so Redis redelivers.
		metrics.RecordEventRejected(region, "buffer_full")
		return
	}

	metrics.RecordEventIngested(region)
	_ = s.client.XAck(ctx, stream, s.cfg.ConsumerGroup, msgID)
}

// streamKey returns the Redis stream key for a region.
func (s *StreamIngestor) streamKey(region string) string {
	return fmt.Sprintf("%s:%s", s.cfg.StreamPrefix, region)
}

// extractBytes converts a Redis field value (string or []byte) to []byte.
func extractBytes(v any) ([]byte, bool) {
	switch t := v.(type) {
	case string:
		return []byte(t), true
	case []byte:
		return t, true
	default:
		return nil, false
	}
}
