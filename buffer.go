package triflestats

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultBufferSize = 256
)

// BufferOptions configures buffered write behavior.
type BufferOptions struct {
	Duration  time.Duration
	Size      int
	Aggregate bool
	Async     bool
}

type bufferedAction struct {
	operation string
	keys      []Key
	values    map[string]any
	count     int64
}

// Buffer batches write operations and flushes them by size and/or time.
type Buffer struct {
	driver      WriteStorage
	countDriver CountDriver

	duration  time.Duration
	size      int
	aggregate bool
	async     bool

	mu             sync.Mutex
	actionsBySig   map[string]*bufferedAction
	actionsLinear  []bufferedAction
	operationCount int
	closed         bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewBuffer creates a write buffer for a driver.
func NewBuffer(driver WriteStorage, opts BufferOptions) *Buffer {
	duration := normalizeBufferDuration(opts.Duration)
	size := normalizeBufferSize(opts.Size)
	countDriver, countAware := driver.(CountDriver)
	aggregate := opts.Aggregate && countAware

	b := &Buffer{
		driver:      driver,
		countDriver: countDriver,
		duration:    duration,
		size:        size,
		aggregate:   aggregate,
		async:       opts.Async,
	}
	b.resetQueueLocked()
	if b.async && b.duration > 0 {
		b.startWorker()
	}
	return b
}

// Inc enqueues an increment operation.
func (b *Buffer) Inc(keys []Key, values map[string]any) error {
	return b.enqueue("inc", keys, values)
}

// Set enqueues a set operation.
func (b *Buffer) Set(keys []Key, values map[string]any) error {
	return b.enqueue("set", keys, values)
}

// Flush drains queued operations and writes them to the driver.
func (b *Buffer) Flush() error {
	actions := b.drainActions()
	if len(actions) == 0 {
		return nil
	}
	for _, action := range actions {
		if err := b.dispatchAction(action); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown stops the worker and flushes outstanding operations.
func (b *Buffer) Shutdown() error {
	if b == nil {
		return nil
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	stopCh := b.stopCh
	b.stopCh = nil
	b.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
		b.wg.Wait()
	}
	return b.Flush()
}

func (b *Buffer) enqueue(operation string, keys []Key, values map[string]any) error {
	if b == nil {
		return fmt.Errorf("buffer is nil")
	}
	if len(keys) == 0 || len(values) == 0 {
		return nil
	}

	shouldFlush := false
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("buffer is closed")
	}
	b.storeActionLocked(operation, keys, values)
	shouldFlush = b.operationCount >= b.size
	b.mu.Unlock()

	if shouldFlush {
		return b.Flush()
	}
	return nil
}

func (b *Buffer) storeActionLocked(operation string, keys []Key, values map[string]any) {
	if b.aggregate {
		sig := signatureFor(operation, keys)
		if existing, ok := b.actionsBySig[sig]; ok {
			switch operation {
			case "inc":
				existing.values = mergeIncrement(existing.values, values)
			case "set":
				existing.values = cloneMap(values)
			}
			existing.count++
			b.operationCount++
			return
		}

		b.actionsBySig[sig] = &bufferedAction{
			operation: operation,
			keys:      cloneKeys(keys),
			values:    cloneMap(values),
			count:     1,
		}
		b.operationCount++
		return
	}

	b.actionsLinear = append(b.actionsLinear, bufferedAction{
		operation: operation,
		keys:      cloneKeys(keys),
		values:    cloneMap(values),
		count:     1,
	})
	b.operationCount++
}

func (b *Buffer) drainActions() []bufferedAction {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.operationCount == 0 {
		return nil
	}

	var actions []bufferedAction
	if b.aggregate {
		actions = make([]bufferedAction, 0, len(b.actionsBySig))
		for _, action := range b.actionsBySig {
			actions = append(actions, *action)
		}
	} else {
		actions = append([]bufferedAction(nil), b.actionsLinear...)
	}
	b.resetQueueLocked()
	return actions
}

func (b *Buffer) resetQueueLocked() {
	b.actionsBySig = map[string]*bufferedAction{}
	b.actionsLinear = []bufferedAction{}
	b.operationCount = 0
}

func (b *Buffer) dispatchAction(action bufferedAction) error {
	if action.count <= 0 {
		action.count = 1
	}

	if b.countDriver != nil {
		switch action.operation {
		case "inc":
			return b.countDriver.IncCount(action.keys, action.values, action.count)
		case "set":
			return b.countDriver.SetCount(action.keys, action.values, action.count)
		default:
			return fmt.Errorf("invalid operation: %s", action.operation)
		}
	}

	repetitions := int(action.count)
	for i := 0; i < repetitions; i++ {
		switch action.operation {
		case "inc":
			if err := b.driver.Inc(action.keys, action.values); err != nil {
				return err
			}
		case "set":
			if err := b.driver.Set(action.keys, action.values); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid operation: %s", action.operation)
		}
	}
	return nil
}

func (b *Buffer) startWorker() {
	stopCh := make(chan struct{})
	b.stopCh = stopCh
	b.wg.Add(1)

	go func(stop <-chan struct{}) {
		defer b.wg.Done()
		ticker := time.NewTicker(b.duration)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_ = b.Flush()
			case <-stop:
				return
			}
		}
	}(stopCh)
}

func (b *Buffer) matches(driver WriteStorage, duration time.Duration, size int, aggregate, async bool) bool {
	if b == nil {
		return false
	}

	normalizedDuration := normalizeBufferDuration(duration)
	normalizedSize := normalizeBufferSize(size)
	normalizedAggregate := aggregate && supportsCountDriver(driver)

	b.mu.Lock()
	defer b.mu.Unlock()
	return sameDriver(b.driver, driver) &&
		b.duration == normalizedDuration &&
		b.size == normalizedSize &&
		b.aggregate == normalizedAggregate &&
		b.async == async
}

func normalizeBufferDuration(value time.Duration) time.Duration {
	if value <= 0 {
		return 0
	}
	return value
}

func normalizeBufferSize(value int) int {
	if value <= 0 {
		return defaultBufferSize
	}
	return value
}

func supportsCountDriver(driver WriteStorage) bool {
	_, ok := driver.(CountDriver)
	return ok
}

func sameDriver(a, b WriteStorage) bool {
	if a == nil || b == nil {
		return a == b
	}

	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)
	if va.IsValid() && vb.IsValid() && va.Type() == vb.Type() {
		switch va.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			return va.Pointer() == vb.Pointer()
		}
	}
	return reflect.DeepEqual(a, b)
}

func signatureFor(operation string, keys []Key) string {
	parts := make([]string, 0, len(keys)+1)
	parts = append(parts, operation)
	for _, key := range keys {
		at := ""
		if key.At != nil {
			at = strconv.FormatInt(key.At.Unix(), 10)
		}
		parts = append(parts, strings.Join([]string{
			key.Prefix,
			key.Key,
			key.Granularity,
			at,
			key.SystemTrackingKey(),
		}, ":"))
	}
	return strings.Join(parts, "|")
}

func cloneKeys(keys []Key) []Key {
	out := make([]Key, 0, len(keys))
	for _, key := range keys {
		clone := key
		if key.At != nil {
			atCopy := *key.At
			clone.At = &atCopy
		}
		out = append(out, clone)
	}
	return out
}

func cloneMap(values map[string]any) map[string]any {
	if values == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value any) any {
	switch node := value.(type) {
	case map[string]any:
		return cloneMap(node)
	case []any:
		out := make([]any, len(node))
		for i, value := range node {
			out[i] = cloneValue(value)
		}
		return out
	default:
		return node
	}
}

func mergeIncrement(current, incoming map[string]any) map[string]any {
	out := cloneMap(current)
	for key, value := range incoming {
		switch node := value.(type) {
		case map[string]any:
			existing, _ := out[key].(map[string]any)
			out[key] = mergeIncrement(existing, node)
		default:
			next := cloneValue(node)
			if delta, ok := toFloat(node); ok {
				base := 0.0
				if existing, ok := toFloat(out[key]); ok {
					base = existing
				}
				next = base + delta
			}
			out[key] = next
		}
	}
	return out
}
