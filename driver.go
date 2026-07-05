package triflestats

import (
	"context"
	"time"
)

// JoinedIdentifier represents identifier mode.
type JoinedIdentifier int

const (
	JoinedFull JoinedIdentifier = iota
	JoinedPartial
	JoinedSeparated
)

// ParseJoinedIdentifier normalizes string/enum into JoinedIdentifier.
// The empty string maps to JoinedFull, mirroring Ruby's default of :full.
func ParseJoinedIdentifier(value string) JoinedIdentifier {
	switch value {
	case "partial", "PARTIAL":
		return JoinedPartial
	case "separated", "SEPARATED":
		return JoinedSeparated
	default:
		return JoinedFull
	}
}

// Driver is the storage backend interface.
type Driver interface {
	Inc(ctx context.Context, keys []Key, values map[string]any) error
	Set(ctx context.Context, keys []Key, values map[string]any) error
	Get(ctx context.Context, keys []Key) ([]map[string]any, error)
	// Ping stores the latest status payload for key (separated mode only;
	// other modes and unsupported drivers are a no-op).
	Ping(ctx context.Context, key Key, values map[string]any) error
	// Scan returns the latest status timestamp and unpacked payload for key.
	// The bool reports whether a status exists.
	Scan(ctx context.Context, key Key) (time.Time, map[string]any, bool, error)
	Description() string
}

// WriteStorage represents the write contract used by Track/Assert.
type WriteStorage interface {
	Inc(ctx context.Context, keys []Key, values map[string]any) error
	Set(ctx context.Context, keys []Key, values map[string]any) error
}

// CountDriver extends drivers with operation-count-aware writes, used by Buffer
// to preserve system tracking counts when multiple operations are aggregated.
type CountDriver interface {
	IncCount(ctx context.Context, keys []Key, values map[string]any, count int64) error
	SetCount(ctx context.Context, keys []Key, values map[string]any, count int64) error
}
