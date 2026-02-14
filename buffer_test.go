package triflestats

import (
	"sync"
	"testing"
	"time"
)

type recordedWrite struct {
	operation string
	keys      []Key
	values    map[string]any
	count     int64
}

type bufferTestDriver struct {
	mu      sync.Mutex
	writes  []recordedWrite
	payload map[string]map[string]any
}

func newBufferTestDriver() *bufferTestDriver {
	return &bufferTestDriver{
		payload: map[string]map[string]any{},
	}
}

type nonCountBufferDriver struct {
	mu     sync.Mutex
	incOps int
	setOps int
}

func (d *nonCountBufferDriver) Inc(keys []Key, values map[string]any) error {
	d.mu.Lock()
	d.incOps++
	d.mu.Unlock()
	return nil
}

func (d *nonCountBufferDriver) Set(keys []Key, values map[string]any) error {
	d.mu.Lock()
	d.setOps++
	d.mu.Unlock()
	return nil
}

func (d *bufferTestDriver) Inc(keys []Key, values map[string]any) error {
	return d.IncCount(keys, values, 1)
}

func (d *bufferTestDriver) Set(keys []Key, values map[string]any) error {
	return d.SetCount(keys, values, 1)
}

func (d *bufferTestDriver) IncCount(keys []Key, values map[string]any, count int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writes = append(d.writes, recordedWrite{
		operation: "inc",
		keys:      cloneKeys(keys),
		values:    cloneMap(values),
		count:     count,
	})
	return nil
}

func (d *bufferTestDriver) SetCount(keys []Key, values map[string]any, count int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writes = append(d.writes, recordedWrite{
		operation: "set",
		keys:      cloneKeys(keys),
		values:    cloneMap(values),
		count:     count,
	})
	return nil
}

func (d *bufferTestDriver) Get(keys []Key) ([]map[string]any, error) {
	return []map[string]any{}, nil
}

func (d *bufferTestDriver) Description() string {
	return "buffer-test-driver"
}

func (d *bufferTestDriver) snapshot() []recordedWrite {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]recordedWrite, len(d.writes))
	copy(out, d.writes)
	return out
}

func TestBuffer_FlushesWhenQueueReachesSize(t *testing.T) {
	driver := newBufferTestDriver()
	buffer := NewBuffer(driver, BufferOptions{
		Duration:  0,
		Size:      2,
		Aggregate: false,
		Async:     false,
	})
	defer func() {
		_ = buffer.Shutdown()
	}()

	now := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
	key := Key{Key: "metric", Granularity: "1h", At: &now}

	if err := buffer.Inc([]Key{key}, map[string]any{"count": 1}); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}
	if err := buffer.Inc([]Key{key}, map[string]any{"count": 2}); err != nil {
		t.Fatalf("second enqueue failed: %v", err)
	}

	writes := driver.snapshot()
	if len(writes) != 2 {
		t.Fatalf("expected 2 writes, got %d", len(writes))
	}
	if writes[0].operation != "inc" || writes[1].operation != "inc" {
		t.Fatalf("unexpected operations: %+v", writes)
	}
}

func TestBuffer_AggregatesIncrementsAndPreservesCount(t *testing.T) {
	driver := newBufferTestDriver()
	buffer := NewBuffer(driver, BufferOptions{
		Duration:  0,
		Size:      10,
		Aggregate: true,
		Async:     false,
	})
	defer func() {
		_ = buffer.Shutdown()
	}()

	now := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
	key := Key{Key: "metric", Granularity: "1h", At: &now}

	_ = buffer.Inc([]Key{key}, map[string]any{"count": 1, "nested": map[string]any{"requests": 1}})
	_ = buffer.Inc([]Key{key}, map[string]any{"count": 2, "nested": map[string]any{"requests": 3}})
	if err := buffer.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	writes := driver.snapshot()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}
	write := writes[0]
	if write.count != 2 {
		t.Fatalf("expected aggregated count 2, got %d", write.count)
	}
	if got := write.values["count"]; got != 3.0 {
		t.Fatalf("expected merged count 3, got %#v", got)
	}
	nested := write.values["nested"].(map[string]any)
	if got := nested["requests"]; got != 4.0 {
		t.Fatalf("expected merged nested.requests 4, got %#v", got)
	}
}

func TestBuffer_AggregatesSetKeepingLastValue(t *testing.T) {
	driver := newBufferTestDriver()
	buffer := NewBuffer(driver, BufferOptions{
		Duration:  0,
		Size:      10,
		Aggregate: true,
		Async:     false,
	})
	defer func() {
		_ = buffer.Shutdown()
	}()

	now := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
	key := Key{Key: "metric", Granularity: "1h", At: &now}

	_ = buffer.Set([]Key{key}, map[string]any{"state": "processing"})
	_ = buffer.Set([]Key{key}, map[string]any{"state": "done", "detail": map[string]any{"attempts": 3}})
	if err := buffer.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	writes := driver.snapshot()
	if len(writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writes))
	}
	write := writes[0]
	if write.operation != "set" {
		t.Fatalf("expected set operation, got %s", write.operation)
	}
	if write.count != 2 {
		t.Fatalf("expected aggregated count 2, got %d", write.count)
	}
	if got := write.values["state"]; got != "done" {
		t.Fatalf("expected last set value, got %#v", got)
	}
}

func TestBuffer_FlushesAutomaticallyOnDuration(t *testing.T) {
	driver := newBufferTestDriver()
	buffer := NewBuffer(driver, BufferOptions{
		Duration:  40 * time.Millisecond,
		Size:      10,
		Aggregate: false,
		Async:     true,
	})
	defer func() {
		_ = buffer.Shutdown()
	}()

	now := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
	key := Key{Key: "metric", Granularity: "1h", At: &now}
	if err := buffer.Inc([]Key{key}, map[string]any{"count": 1}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	time.Sleep(120 * time.Millisecond)

	writes := driver.snapshot()
	if len(writes) != 1 {
		t.Fatalf("expected async flush write, got %d", len(writes))
	}
}

func TestBuffer_ShutdownFlushesOutstandingWrites(t *testing.T) {
	driver := newBufferTestDriver()
	buffer := NewBuffer(driver, BufferOptions{
		Duration:  0,
		Size:      10,
		Aggregate: false,
		Async:     false,
	})

	now := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
	key := Key{Key: "metric", Granularity: "1h", At: &now}
	if err := buffer.Inc([]Key{key}, map[string]any{"count": 7}); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if err := buffer.Shutdown(); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	writes := driver.snapshot()
	if len(writes) != 1 {
		t.Fatalf("expected shutdown flush write, got %d", len(writes))
	}
	if writes[0].count != 1 {
		t.Fatalf("expected write count 1, got %d", writes[0].count)
	}
}

func TestBuffer_FallsBackToLinearQueueForNonCountDrivers(t *testing.T) {
	driver := &nonCountBufferDriver{}
	buffer := NewBuffer(driver, BufferOptions{
		Duration:  0,
		Size:      2,
		Aggregate: true,
		Async:     false,
	})
	defer func() {
		_ = buffer.Shutdown()
	}()

	if buffer.aggregate {
		t.Fatalf("expected aggregate to be disabled for non-count-aware drivers")
	}

	now := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
	key := Key{Key: "metric", Granularity: "1h", At: &now}
	_ = buffer.Inc([]Key{key}, map[string]any{"count": 1})
	_ = buffer.Inc([]Key{key}, map[string]any{"count": 2})

	driver.mu.Lock()
	defer driver.mu.Unlock()
	if driver.incOps != 2 {
		t.Fatalf("expected two inc dispatches, got %d", driver.incOps)
	}
}

func TestBuffer_ClosedBufferRejectsEnqueue(t *testing.T) {
	driver := newBufferTestDriver()
	buffer := NewBuffer(driver, BufferOptions{
		Duration:  0,
		Size:      10,
		Aggregate: false,
		Async:     false,
	})
	if err := buffer.Shutdown(); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	now := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
	key := Key{Key: "metric", Granularity: "1h", At: &now}
	if err := buffer.Inc([]Key{key}, map[string]any{"count": 1}); err == nil {
		t.Fatalf("expected enqueue error after shutdown")
	}
}
