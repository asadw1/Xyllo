package dlq

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yourusername/xyllo/config"
)

// newFileBackedDLQ creates a DLQ backed by a temp file, returning the DLQ and
// the path to the file.  The file and DLQ are closed via t.Cleanup.
func newFileBackedDLQ(t *testing.T) (*DLQ, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dlq.jsonl")
	d, err := New(config.DLQConfig{Backend: "file", Target: path})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, path
}

// readEntries parses all newline-delimited JSON entries from path.
func readEntries(t *testing.T, path string) []Entry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open dlq file: %v", err)
	}
	defer f.Close()

	var entries []Entry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("unmarshal entry: %v", err)
		}
		entries = append(entries, e)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan dlq file: %v", err)
	}
	return entries
}

// ── New ──────────────────────────────────────────────────────────────────────

func TestNew_FileBackend_EmptyTarget_NoError(t *testing.T) {
	d, err := New(config.DLQConfig{Backend: "file", Target: ""})
	if err != nil {
		t.Fatalf("expected no error for empty target, got: %v", err)
	}
	if d.file != nil {
		t.Error("file should be nil when Target is empty")
	}
}

func TestNew_FileBackend_ValidPath_OpensFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dlq.jsonl")
	d, err := New(config.DLQConfig{Backend: "file", Target: path})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer d.Close()
	if d.file == nil {
		t.Error("expected file to be non-nil for valid target path")
	}
}

func TestNew_FileBackend_InvalidPath_ReturnsError(t *testing.T) {
	_, err := New(config.DLQConfig{Backend: "file", Target: "/nonexistent/path/dlq.jsonl"})
	if err == nil {
		t.Fatal("expected error for invalid path, got nil")
	}
	if !strings.Contains(err.Error(), "dlq: open file") {
		t.Errorf("error should describe file open failure, got: %v", err)
	}
}

func TestNew_LogBackend_NoFileOpened(t *testing.T) {
	d, err := New(config.DLQConfig{Backend: "log", Target: ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.file != nil {
		t.Error("file should be nil for non-file backend")
	}
}

// ── Push ─────────────────────────────────────────────────────────────────────

func TestPush_WritesEntryToFile(t *testing.T) {
	d, path := newFileBackedDLQ(t)

	d.Push("web", "missing field id", []byte(`{"raw":"data"}`))

	entries := readEntries(t, path)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Source != "web" {
		t.Errorf("Source: want %q, got %q", "web", e.Source)
	}
	if e.Reason != "missing field id" {
		t.Errorf("Reason: want %q, got %q", "missing field id", e.Reason)
	}
	if string(e.Raw) != `{"raw":"data"}` {
		t.Errorf("Raw: want %q, got %q", `{"raw":"data"}`, string(e.Raw))
	}
	if e.ReceivedAt.IsZero() {
		t.Error("ReceivedAt should not be zero")
	}
}

func TestPush_WritesMultipleEntriesAsNewlineDelimitedJSON(t *testing.T) {
	d, path := newFileBackedDLQ(t)

	d.Push("mobile", "bad type", []byte(`{"a":1}`))
	d.Push("web", "zero timestamp", []byte(`{"b":2}`))
	d.Push("sdk", "null field", []byte(`{"c":3}`))

	entries := readEntries(t, path)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	sources := []string{"mobile", "web", "sdk"}
	for i, e := range entries {
		if e.Source != sources[i] {
			t.Errorf("entry %d Source: want %q, got %q", i, sources[i], e.Source)
		}
	}
}

func TestPush_FallsBackToLogWhenNoFile(t *testing.T) {
	// No file configured — Push should not panic and should not return an error.
	d, err := New(config.DLQConfig{Backend: "file", Target: ""})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// This call should log instead of writing to a file — no panic is sufficient.
	d.Push("test", "no file backend", []byte(`{}`))
}

func TestPush_ConcurrentWritesAreThreadSafe(t *testing.T) {
	d, path := newFileBackedDLQ(t)

	const goroutines = 20
	const perGoroutine = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				d.Push("concurrent", "race test", []byte(`{}`))
			}
		}()
	}
	wg.Wait()
	_ = d.Close()

	entries := readEntries(t, path)
	if len(entries) != goroutines*perGoroutine {
		t.Errorf("expected %d entries, got %d", goroutines*perGoroutine, len(entries))
	}
}

// ── Close ────────────────────────────────────────────────────────────────────

func TestClose_NilFile_NoError(t *testing.T) {
	d, err := New(config.DLQConfig{Backend: "file", Target: ""})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("Close with nil file should return nil, got: %v", err)
	}
}

func TestClose_ClearsFileReference(t *testing.T) {
	d, _ := newFileBackedDLQ(t)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d.file != nil {
		t.Error("file reference should be nil after Close")
	}
}

func TestClose_AfterClose_PushFallsBackToLog(t *testing.T) {
	d, path := newFileBackedDLQ(t)
	_ = d.Close()

	// Push after close must not panic; it should log instead of writing to file.
	d.Push("test", "post-close push", []byte(`{}`))

	// File should contain zero entries since the file was closed before Push.
	entries := readEntries(t, path)
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after close, got %d", len(entries))
	}
}
