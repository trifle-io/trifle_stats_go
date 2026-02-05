package triflestats

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

func toFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, false
		}
		return v, true
	case float32:
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case int16:
		return float64(v), true
	case int8:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint8:
		return float64(v), true
	case json.Number:
		if f, err := v.Float64(); err == nil {
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return 0, false
			}
			return f, true
		}
	case string:
		if f, ok := parseNumericString(v); ok {
			return f, true
		}
	}
	return 0, false
}

func parseNumericString(value string) (float64, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, false
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// NormalizeNumeric returns a float64 for numeric inputs or nil otherwise.
func NormalizeNumeric(value any) any {
	if f, ok := toFloat(value); ok {
		return f
	}
	return nil
}

func toFloatDefault(value any, fallback float64) float64 {
	if f, ok := toFloat(value); ok {
		return f
	}
	return fallback
}
