package triflestats

import (
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
