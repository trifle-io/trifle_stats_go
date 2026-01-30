package triflestats

import (
	"strings"
	"time"
)

// TimelinePoint is a single timeline entry.
type TimelinePoint struct {
	At    time.Time `json:"at"`
	Value float64   `json:"value"`
}

// TimelineTransform optionally transforms timeline entries.
type TimelineTransform func(time.Time, any) any

// CategoryTransform optionally transforms category entries.
// Return the key and value to record. If key is empty, the original key is used.
type CategoryTransform func(string, any) (string, any)

// FormatTimeline builds a timeline map keyed by path.
func (s Series) FormatTimeline(path string, slices int, transform TimelineTransform) map[string]any {
	if len(s.At) == 0 {
		return map[string]any{}
	}

	segments := SplitPath(path)
	resolved := ResolveConcretePaths(s.Values, segments)
	zipped := zipSeries(s)

	result := map[string]any{}
	for _, pathSegments := range resolved {
		fullKey := joinSegments(pathSegments)
		entries := make([]any, 0, len(zipped))
		for _, item := range zipped {
			value := fetchPath(item.values, pathSegments)
			if transform != nil {
				entries = append(entries, transform(item.at, value))
			} else {
				entries = append(entries, TimelinePoint{
					At:    item.at,
					Value: toFloatDefault(value, 0),
				})
			}
		}
		groups := sliceValues(entries, slices)
		if slices <= 1 {
			result[fullKey] = groups[0]
		} else {
			result[fullKey] = groups
		}
	}
	return result
}

// FormatCategory builds a category aggregation map keyed by path.
func (s Series) FormatCategory(path string, slices int, transform CategoryTransform) any {
	values := s.Values
	if len(values) == 0 {
		return map[string]any{}
	}

	segments := SplitPath(path)
	resolved := ResolveConcretePaths(values, segments)
	groups := sliceValues(toAnySlice(values), slices)

	aggregated := make([]map[string]float64, 0, len(groups))
	for _, group := range groups {
		sliceValues, ok := anySliceToMapSlice(group)
		if !ok {
			continue
		}
		aggregated = append(aggregated, aggregateCategorySlice(sliceValues, resolved, transform))
	}

	if slices <= 1 {
		if len(aggregated) == 0 {
			return map[string]any{}
		}
		return mapStringFloatToAny(aggregated[0])
	}

	out := make([]map[string]any, 0, len(aggregated))
	for _, entry := range aggregated {
		out = append(out, mapStringFloatToAny(entry))
	}
	return out
}

type zippedEntry struct {
	at     time.Time
	values map[string]any
}

func zipSeries(series Series) []zippedEntry {
	count := len(series.At)
	if len(series.Values) < count {
		count = len(series.Values)
	}
	out := make([]zippedEntry, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, zippedEntry{at: series.At[i], values: series.Values[i]})
	}
	return out
}

func aggregateCategorySlice(values []map[string]any, paths [][]string, transform CategoryTransform) map[string]float64 {
	acc := map[string]float64{}
	for _, data := range values {
		for _, pathSegments := range paths {
			fullKey := joinSegments(pathSegments)
			raw := fetchPath(data, pathSegments)
			key := fullKey
			value := raw
			if transform != nil {
				if tkey, tval := transform(fullKey, raw); tkey != "" {
					key = tkey
					value = tval
				}
			}
			acc[key] += toFloatDefault(value, 0)
		}
	}
	return acc
}

func joinSegments(parts []string) string {
	return strings.Join(parts, ".")
}

func toAnySlice(values []map[string]any) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func anySliceToMapSlice(values []any) ([]map[string]any, bool) {
	out := make([]map[string]any, 0, len(values))
	for _, value := range values {
		row, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		out = append(out, row)
	}
	return out, true
}

func mapStringFloatToAny(values map[string]float64) map[string]any {
	out := map[string]any{}
	for key, value := range values {
		out[key] = value
	}
	return out
}
