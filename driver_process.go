package triflestats

import (
	"context"
	"sync"
	"time"
)

type processStatus struct {
	at     time.Time
	values map[string]any
}

// ProcessDriver is an in-memory driver mirroring Ruby's Process driver.
// It stores packed values under joined string keys and is intended for
// testing and single-process consumers.
type ProcessDriver struct {
	Separator string

	mu       sync.Mutex
	data     map[string]map[string]any
	statuses map[string]processStatus
}

// NewProcessDriver creates an in-memory driver.
func NewProcessDriver() *ProcessDriver {
	return &ProcessDriver{
		Separator: "::",
		data:      map[string]map[string]any{},
		statuses:  map[string]processStatus{},
	}
}

func (d *ProcessDriver) Description() string {
	return "ProcessDriver(J)"
}

// Inc increments numeric values in-place.
func (d *ProcessDriver) Inc(ctx context.Context, keys []Key, values map[string]any) error {
	return d.IncCount(ctx, keys, values, 1)
}

// IncCount increments values; count is ignored (no system tracking).
func (d *ProcessDriver) IncCount(_ context.Context, keys []Key, values map[string]any, _ int64) error {
	packed := Pack(values)

	d.mu.Lock()
	defer d.mu.Unlock()
	for _, key := range keys {
		joined := key.Join(d.Separator)
		bucket := d.data[joined]
		if bucket == nil {
			bucket = map[string]any{}
			d.data[joined] = bucket
		}
		for field, value := range packed {
			bucket[field] = toFloatDefault(bucket[field], 0) + toFloatDefault(value, 0)
		}
	}
	return nil
}

// Set assigns values per-field without deleting unspecified fields.
func (d *ProcessDriver) Set(ctx context.Context, keys []Key, values map[string]any) error {
	return d.SetCount(ctx, keys, values, 1)
}

// SetCount assigns values; count is ignored (no system tracking).
func (d *ProcessDriver) SetCount(_ context.Context, keys []Key, values map[string]any, _ int64) error {
	packed := Pack(values)

	d.mu.Lock()
	defer d.mu.Unlock()
	for _, key := range keys {
		joined := key.Join(d.Separator)
		bucket := d.data[joined]
		if bucket == nil {
			bucket = map[string]any{}
			d.data[joined] = bucket
		}
		for field, value := range packed {
			// Normalize numerics to float64 so reads match the JSON-backed drivers.
			if number, ok := toFloat(value); ok {
				bucket[field] = number
				continue
			}
			bucket[field] = cloneValue(value)
		}
	}
	return nil
}

// Get returns unpacked copies of stored values in key order.
func (d *ProcessDriver) Get(_ context.Context, keys []Key) ([]map[string]any, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	results := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		packed := d.data[key.Join(d.Separator)]
		results = append(results, Unpack(cloneMap(packed)))
	}
	return results, nil
}

// Ping stores the latest status payload for the base key.
func (d *ProcessDriver) Ping(_ context.Context, key Key, values map[string]any) error {
	at := time.Now()
	if key.At != nil {
		at = *key.At
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.statuses[key.Key] = processStatus{
		at:     at,
		values: Pack(values),
	}
	return nil
}

// Scan returns the latest status stored for the base key.
func (d *ProcessDriver) Scan(_ context.Context, key Key) (time.Time, map[string]any, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	status, ok := d.statuses[key.Key]
	if !ok {
		return time.Time{}, nil, false, nil
	}
	return status.at, Unpack(cloneMap(status.values)), true, nil
}
