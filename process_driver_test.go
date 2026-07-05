package triflestats

import (
	"context"
	"testing"
	"time"
)

func TestProcessDriver_IncSetGet(t *testing.T) {
	driver := NewProcessDriver()
	ctx := context.Background()

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}

	if err := driver.Inc(ctx, []Key{key}, map[string]any{"count": 1, "meta": map[string]any{"duration": 2}}); err != nil {
		t.Fatalf("inc failed: %v", err)
	}
	if err := driver.Inc(ctx, []Key{key}, map[string]any{"count": 2}); err != nil {
		t.Fatalf("second inc failed: %v", err)
	}
	if err := driver.Set(ctx, []Key{key}, map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	values, err := driver.Get(ctx, []Key{key})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("expected one row, got %d", len(values))
	}
	row := values[0]
	if got := row["count"]; got != float64(3) {
		t.Fatalf("expected count 3, got %#v", got)
	}
	meta := row["meta"].(map[string]any)
	if got := meta["duration"]; got != float64(2) {
		t.Fatalf("expected meta.duration 2, got %#v", got)
	}
	if got := row["status"]; got != "ok" {
		t.Fatalf("expected status ok, got %#v", got)
	}

	missing := Key{Key: "missing", Granularity: "1h", At: &at}
	values, err = driver.Get(ctx, []Key{missing})
	if err != nil {
		t.Fatalf("get missing failed: %v", err)
	}
	if len(values[0]) != 0 {
		t.Fatalf("expected empty map for missing key, got %+v", values[0])
	}
}

func TestProcessDriver_GetReturnsCopies(t *testing.T) {
	driver := NewProcessDriver()
	ctx := context.Background()

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}
	if err := driver.Set(ctx, []Key{key}, map[string]any{"count": 1}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	first, _ := driver.Get(ctx, []Key{key})
	first[0]["count"] = float64(999)

	second, _ := driver.Get(ctx, []Key{key})
	if got := second[0]["count"]; got != float64(1) {
		t.Fatalf("expected stored value unchanged, got %#v", got)
	}
}

func TestProcessDriver_PingScan(t *testing.T) {
	driver := NewProcessDriver()
	ctx := context.Background()

	_, _, found, err := driver.Scan(ctx, Key{Key: "jobs"})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if found {
		t.Fatalf("expected no status before ping")
	}

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	if err := driver.Ping(ctx, Key{Key: "jobs", At: &at}, map[string]any{"state": "ok"}); err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	scanAt, values, found, err := driver.Scan(ctx, Key{Key: "jobs"})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if !found {
		t.Fatalf("expected status after ping")
	}
	if !scanAt.Equal(at) {
		t.Fatalf("expected at %v, got %v", at, scanAt)
	}
	if got := values["state"]; got != "ok" {
		t.Fatalf("expected state ok, got %#v", got)
	}
}

func TestProcessDriver_Description(t *testing.T) {
	driver := NewProcessDriver()
	if got := driver.Description(); got != "ProcessDriver(J)" {
		t.Fatalf("unexpected description: %s", got)
	}
}
