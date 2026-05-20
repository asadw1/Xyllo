package batcher_test

import (
	"sync"
	"testing"
	"time"

	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/batcher"
)

func newBatcher(maxSize int) *batcher.Batcher {
	return batcher.New(config.BatcherConfig{
		MaxSize:       maxSize,
		FlushInterval: time.Minute, // large — tests drive flushes explicitly
	})
}

func TestBatcher_FlushEmptyIsNoOp(t *testing.T) {
	b := newBatcher(10)
	defer b.Stop()

	b.Flush() // must not panic
	b.Flush() // idempotent
}

func TestBatcher_AddThenFlush(t *testing.T) {
	b := newBatcher(10)
	defer b.Stop()

	b.Add([]byte(`"a"`))
	b.Add([]byte(`"b"`))
	b.Flush() // drains the buffer

	// Buffer is now empty; a second flush must be a no-op.
	b.Flush()
}

func TestBatcher_AutoFlushAtMaxSize(t *testing.T) {
	b := newBatcher(3)
	defer b.Stop()

	b.Add([]byte(`"x"`))
	b.Add([]byte(`"y"`))
	b.Add([]byte(`"z"`)) // third item hits MaxSize — triggers automatic flush

	// Buffer was drained by the auto-flush; explicit flush is a no-op.
	b.Flush()
}

func TestBatcher_MaxSizeOfOne(t *testing.T) {
	b := newBatcher(1)
	defer b.Stop()

	b.Add([]byte(`"solo"`)) // immediately flushes
	b.Flush()               // no-op
}

func TestBatcher_Stop(t *testing.T) {
	b := batcher.New(config.BatcherConfig{
		MaxSize:       10,
		FlushInterval: 10 * time.Millisecond,
	})
	b.Stop() // stopping before any Adds must not panic
}

func TestBatcher_ConcurrentAdd(t *testing.T) {
	b := newBatcher(5)
	defer b.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Add([]byte(`"concurrent"`))
		}()
	}
	wg.Wait()
	b.Flush() // drain whatever remains
}

func TestBatcher_FlushAfterMultipleAutoFlushes(t *testing.T) {
	b := newBatcher(2)
	defer b.Stop()

	// Two rounds of auto-flush followed by a manual flush on the remainder.
	b.Add([]byte(`"1"`))
	b.Add([]byte(`"2"`)) // auto-flush
	b.Add([]byte(`"3"`))
	b.Add([]byte(`"4"`)) // auto-flush
	b.Add([]byte(`"5"`))
	b.Flush() // drains the last item
}
