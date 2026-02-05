package triflestats

import (
	"strconv"
	"strings"
	"time"
)

// Key represents a structured timeseries key.
type Key struct {
	Prefix      string
	Key         string
	TrackingKey string
	Granularity string
	At          *time.Time
}

// SystemTrackingKey returns the key used for system tracking.
func (k Key) SystemTrackingKey() string {
	if k.TrackingKey != "" {
		return k.TrackingKey
	}
	return k.Key
}

// Join returns the full joined identifier (prefix, key, granularity, at unix).
func (k Key) Join(separator string) string {
	parts := make([]string, 0, 4)
	if k.Prefix != "" {
		parts = append(parts, k.Prefix)
	}
	if k.Key != "" {
		parts = append(parts, k.Key)
	}
	if k.Granularity != "" {
		parts = append(parts, k.Granularity)
	}
	if k.At != nil {
		parts = append(parts, strconv.FormatInt(k.At.Unix(), 10))
	}
	return strings.Join(parts, separator)
}

// PartialJoin returns prefix+key+granularity without timestamp.
func (k Key) PartialJoin(separator string) string {
	parts := make([]string, 0, 3)
	if k.Prefix != "" {
		parts = append(parts, k.Prefix)
	}
	if k.Key != "" {
		parts = append(parts, k.Key)
	}
	if k.Granularity != "" {
		parts = append(parts, k.Granularity)
	}
	return strings.Join(parts, separator)
}

// Identifier returns identifier map based on join mode.
func (k Key) Identifier(separator string, mode JoinedIdentifier) map[string]any {
	switch mode {
	case JoinedSeparated:
		out := map[string]any{
			"key":         k.Key,
			"granularity": k.Granularity,
		}
		if k.At != nil {
			out["at"] = *k.At
		}
		cleanNil(out)
		return out
	case JoinedPartial:
		out := map[string]any{
			"key": k.PartialJoin(separator),
		}
		if k.At != nil {
			out["at"] = *k.At
		}
		cleanNil(out)
		return out
	case JoinedFull:
		return map[string]any{"key": k.Join(separator)}
	default:
		return map[string]any{"key": k.Join(separator)}
	}
}

func cleanNil(m map[string]any) {
	for k, v := range m {
		if v == nil {
			delete(m, k)
		}
	}
}
