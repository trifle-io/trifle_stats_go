package triflestats

import (
	"context"
	"testing"
	"time"
)

func TestOpsTrackAssertValues(t *testing.T) {
	db := newTestDB(t)
	driver := NewSQLiteDriver(db, "trifle_stats", JoinedFull)
	if err := driver.Setup(context.Background()); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.TimeZone = "UTC"
	cfg.Granularities = []string{"1d"}
	cfg.BufferEnabled = false

	at := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

	if err := Track(context.Background(), cfg, "event::logs", at, map[string]any{"count": 1}); err != nil {
		t.Fatalf("track failed: %v", err)
	}
	if err := Assert(context.Background(), cfg, "event::logs", at, map[string]any{"count": 5}); err != nil {
		t.Fatalf("assert failed: %v", err)
	}

	from := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 15, 23, 59, 0, 0, time.UTC)
	result, err := Values(context.Background(), cfg, "event::logs", from, to, "1d", false)
	if err != nil {
		t.Fatalf("values failed: %v", err)
	}
	if len(result.At) != 1 || len(result.Values) != 1 {
		t.Fatalf("unexpected result lengths: %+v", result)
	}

	countVal, ok := result.Values[0]["count"].(float64)
	if !ok || countVal != 5 {
		t.Fatalf("expected count 5, got %+v", result.Values[0])
	}
}

func TestOpsBeamAndScan(t *testing.T) {
	db := newTestDB(t)
	driver := NewSQLiteDriver(db, "trifle_stats", JoinedSeparated)
	if err := driver.Setup(context.Background()); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.TimeZone = "UTC"
	cfg.BufferEnabled = false

	ctx := context.Background()

	before, err := Scan(ctx, cfg, "jobs::process")
	if err != nil {
		t.Fatalf("scan before beam failed: %v", err)
	}
	if before.Found {
		t.Fatalf("expected no status before beam, got %+v", before)
	}

	at := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	if err := Beam(ctx, cfg, "jobs::process", at, map[string]any{"state": "running", "progress": 42}); err != nil {
		t.Fatalf("beam failed: %v", err)
	}

	// A later beam replaces the stored status.
	later := at.Add(15 * time.Minute)
	if err := Beam(ctx, cfg, "jobs::process", later, map[string]any{"state": "done"}); err != nil {
		t.Fatalf("second beam failed: %v", err)
	}

	result, err := Scan(ctx, cfg, "jobs::process")
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if !result.Found {
		t.Fatalf("expected status after beam")
	}
	if !result.At.Equal(later) {
		t.Fatalf("expected scan at %v, got %v", later, result.At)
	}
	data, ok := result.Values["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data key in scan values, got %+v", result.Values)
	}
	if got := data["state"]; got != "done" {
		t.Fatalf("expected state done, got %#v", got)
	}
	if got := result.Values["at"]; got != float64(later.Unix()) {
		t.Fatalf("expected at %d in scan values, got %#v", later.Unix(), got)
	}
}

func TestOpsBeamScanNoOpForJoinedMode(t *testing.T) {
	db := newTestDB(t)
	driver := NewSQLiteDriver(db, "trifle_stats", JoinedFull)
	if err := driver.Setup(context.Background()); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = false

	ctx := context.Background()
	at := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	if err := Beam(ctx, cfg, "jobs", at, map[string]any{"state": "ok"}); err != nil {
		t.Fatalf("expected beam no-op, got %v", err)
	}
	result, err := Scan(ctx, cfg, "jobs")
	if err != nil {
		t.Fatalf("expected scan no-op, got %v", err)
	}
	if result.Found {
		t.Fatalf("expected no status in joined mode")
	}
}
