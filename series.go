package triflestats

import (
	"sort"
	"strconv"
	"time"
)

// Series wraps time-series data for aggregation/formatting.
type Series struct {
	At     []time.Time
	Values []map[string]any
}

// NewSeries builds a Series from timestamps and values.
func NewSeries(at []time.Time, values []map[string]any) Series {
	return Series{
		At:     normalizeTimes(at),
		Values: normalizeValues(values),
	}
}

// SeriesFromResult converts ValuesResult into a Series.
func SeriesFromResult(result ValuesResult) Series {
	return NewSeries(result.At, result.Values)
}

// FetchPath returns the value at a dotted path.
func FetchPath(data map[string]any, path string) any {
	return fetchPath(data, SplitPath(path))
}

// AvailablePaths returns sorted numeric value paths across the series.
func (s Series) AvailablePaths() []string {
	seen := map[string]struct{}{}
	for _, row := range s.Values {
		flattenNumericPaths(row, "", seen)
	}
	paths := make([]string, 0, len(seen))
	for key := range seen {
		paths = append(paths, key)
	}
	sort.Strings(paths)
	return paths
}

func normalizeTimes(values []time.Time) []time.Time {
	if values == nil {
		return []time.Time{}
	}
	return values
}

func normalizeValues(values []map[string]any) []map[string]any {
	if values == nil {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(values))
	for _, row := range values {
		if row == nil {
			out = append(out, map[string]any{})
		} else {
			out = append(out, row)
		}
	}
	return out
}

func flattenNumericPaths(value any, prefix string, out map[string]struct{}) {
	switch node := value.(type) {
	case map[string]any:
		for key, child := range node {
			next := joinPath(prefix, key)
			flattenNumericPaths(child, next, out)
		}
	case []any:
		for idx, child := range node {
			next := joinPath(prefix, strconv.Itoa(idx))
			flattenNumericPaths(child, next, out)
		}
	default:
		if prefix == "" {
			return
		}
		if _, ok := toFloat(node); ok {
			out[prefix] = struct{}{}
		}
	}
}

func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
