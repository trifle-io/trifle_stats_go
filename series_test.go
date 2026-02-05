package triflestats

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestSeriesAvailablePaths(t *testing.T) {
	series := NewSeries(nil, []map[string]any{
		{
			"count": 1,
			"meta": map[string]any{
				"duration": 2,
				"label":    "fast",
			},
		},
		{
			"count": 2,
			"country": map[string]any{
				"US": 3,
			},
		},
	})

	got := series.AvailablePaths()
	expect := []string{"count", "country.US", "meta.duration"}
	if !reflect.DeepEqual(got, expect) {
		t.Fatalf("expected %v, got %v", expect, got)
	}
}

func TestSeriesNormalizesNumericStrings(t *testing.T) {
	series := NewSeries(nil, []map[string]any{
		{
			"sum":   "108451730347.56950000000010",
			"count": json.Number("12"),
			"nested": map[string]any{
				"value": "3.5",
			},
			"label": "ok",
		},
	})

	if _, ok := series.Values[0]["sum"].(string); ok {
		t.Fatalf("expected sum to be normalized to numeric value")
	}
	if _, ok := toFloat(series.Values[0]["sum"]); !ok {
		t.Fatalf("expected sum to be numeric")
	}

	nested, ok := series.Values[0]["nested"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map")
	}
	if _, ok := nested["value"].(string); ok {
		t.Fatalf("expected nested value to be normalized to numeric value")
	}
	if _, ok := toFloat(nested["value"]); !ok {
		t.Fatalf("expected nested value to be numeric")
	}
	if _, ok := series.Values[0]["label"].(string); !ok {
		t.Fatalf("expected label to remain string")
	}
}

func TestAggregators(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	series := NewSeries([]time.Time{now, now, now, now}, []map[string]any{
		{"count": 1},
		{"count": 2},
		{},
		{"count": 4},
	})

	sum := series.AggregateSum("count", 2)
	if len(sum) != 2 || sum[0] != float64(3) || sum[1] != float64(4) {
		t.Fatalf("unexpected sum result: %#v", sum)
	}

	mean := series.AggregateMean("count", 2)
	if len(mean) != 2 || mean[0] != float64(1.5) || mean[1] != float64(4) {
		t.Fatalf("unexpected mean result: %#v", mean)
	}

	min := series.AggregateMin("count", 2)
	if len(min) != 2 || min[0] != float64(1) || min[1] != float64(4) {
		t.Fatalf("unexpected min result: %#v", min)
	}

	max := series.AggregateMax("count", 2)
	if len(max) != 2 || max[0] != float64(2) || max[1] != float64(4) {
		t.Fatalf("unexpected max result: %#v", max)
	}
}

func TestFormatters(t *testing.T) {
	at := []time.Time{
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	series := NewSeries(at, []map[string]any{
		{"metrics": map[string]any{"a": 1, "b": 2}},
		{"metrics": map[string]any{"a": 2, "b": 3}},
	})

	timeline := series.FormatTimeline("metrics", 1, nil)
	if _, ok := timeline["metrics.a"]; !ok {
		t.Fatalf("expected metrics.a in timeline")
	}
	if _, ok := timeline["metrics.b"]; !ok {
		t.Fatalf("expected metrics.b in timeline")
	}

	category := series.FormatCategory("metrics", 1, nil)
	categoryMap, ok := category.(map[string]any)
	if !ok {
		t.Fatalf("expected category map, got %T", category)
	}
	if categoryMap["metrics.a"] != float64(3) || categoryMap["metrics.b"] != float64(5) {
		t.Fatalf("unexpected category map: %#v", categoryMap)
	}
}

func TestTransponderAdd(t *testing.T) {
	series := NewSeries(nil, []map[string]any{
		{"left": 2, "right": 3},
	})

	updated := series.TransformAdd("left", "right", "sum")
	if len(updated.Values) != 1 {
		t.Fatalf("expected 1 value row")
	}
	if updated.Values[0]["sum"] != float64(5) {
		t.Fatalf("unexpected transponder result: %#v", updated.Values[0])
	}
}
