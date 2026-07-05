package triflestats

import (
	"context"
	"fmt"
	"time"
)

// ValuesResult mirrors Ruby/Elixir return structure.
type ValuesResult struct {
	At     []time.Time
	Values []map[string]any
}

// ScanResult mirrors Ruby's Operations::Status::Scan return structure.
type ScanResult struct {
	At     time.Time
	Values map[string]any
	Found  bool
}

const untrackedKeyName = "__untracked__"

type trackOptions struct {
	trackingKey string
}

// TrackOption configures Track/Assert behavior.
type TrackOption func(*trackOptions)

// Untracked routes system tracking to a shared "__untracked__" key.
func Untracked() TrackOption {
	return func(opts *trackOptions) {
		opts.trackingKey = untrackedKeyName
	}
}

// Track increments values across configured granularities.
func Track(ctx context.Context, cfg *Config, key string, at time.Time, values map[string]any, opts ...TrackOption) error {
	return trackOrAssert(ctx, cfg, key, at, values, "inc", opts...)
}

// Assert sets values across configured granularities.
func Assert(ctx context.Context, cfg *Config, key string, at time.Time, values map[string]any, opts ...TrackOption) error {
	return trackOrAssert(ctx, cfg, key, at, values, "set", opts...)
}

func trackOrAssert(ctx context.Context, cfg *Config, key string, at time.Time, values map[string]any, op string, opts ...TrackOption) error {
	if cfg == nil {
		return fmt.Errorf("config required")
	}
	storage := cfg.Storage()
	if storage == nil {
		return fmt.Errorf("config driver required")
	}

	optState := trackOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&optState)
		}
	}

	granularities := cfg.EffectiveGranularities()
	keys := make([]Key, 0, len(granularities))
	for _, g := range granularities {
		parser := NewParser(g)
		if !parser.Valid() {
			continue
		}
		nocturnal := NewNocturnal(at, cfg)
		floored := nocturnal.Floor(parser.Offset, parser.Unit)
		keys = append(keys, Key{
			Key:         key,
			TrackingKey: optState.trackingKey,
			Granularity: g,
			At:          &floored,
		})
	}

	switch op {
	case "inc":
		return storage.Inc(ctx, keys, values)
	case "set":
		return storage.Set(ctx, keys, values)
	default:
		return fmt.Errorf("invalid op")
	}
}

// Values retrieves time series values for a granularity.
func Values(ctx context.Context, cfg *Config, key string, from, to time.Time, granularity string, skipBlanks bool) (ValuesResult, error) {
	if cfg == nil || cfg.Driver == nil {
		return ValuesResult{}, fmt.Errorf("config and driver required")
	}

	parser := NewParser(granularity)
	if !parser.Valid() {
		return ValuesResult{}, fmt.Errorf("invalid granularity: %s", granularity)
	}

	timeline := Timeline(from, to, parser.Offset, parser.Unit, cfg)
	keys := make([]Key, 0, len(timeline))
	for _, at := range timeline {
		atCopy := at
		keys = append(keys, Key{
			Key:         key,
			Granularity: granularity,
			At:          &atCopy,
		})
	}

	valuesList, err := cfg.Driver.Get(ctx, keys)
	if err != nil {
		return ValuesResult{}, err
	}

	result := ValuesResult{
		At:     timeline,
		Values: valuesList,
	}

	if skipBlanks {
		clean := ValuesResult{At: []time.Time{}, Values: []map[string]any{}}
		for i, v := range result.Values {
			if len(v) == 0 {
				continue
			}
			clean.At = append(clean.At, result.At[i])
			clean.Values = append(clean.Values, v)
		}
		return clean, nil
	}

	return result, nil
}

// Beam stores a status payload for key at the given time. It bypasses the
// buffer and writes through the configured driver directly.
func Beam(ctx context.Context, cfg *Config, key string, at time.Time, values map[string]any) error {
	if cfg == nil || cfg.Driver == nil {
		return fmt.Errorf("config and driver required")
	}
	return cfg.Driver.Ping(ctx, Key{Key: key, At: &at}, values)
}

// Scan retrieves the latest status payload for key. It bypasses the buffer
// and reads through the configured driver directly.
func Scan(ctx context.Context, cfg *Config, key string) (ScanResult, error) {
	if cfg == nil || cfg.Driver == nil {
		return ScanResult{}, fmt.Errorf("config and driver required")
	}
	at, values, found, err := cfg.Driver.Scan(ctx, Key{Key: key})
	if err != nil {
		return ScanResult{}, err
	}
	return ScanResult{At: at, Values: values, Found: found}, nil
}
