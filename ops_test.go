package triflestats

import (
	"testing"
	"time"
)

func TestOpsTrackAssertValues(t *testing.T) {
	db := newTestDB(t)
	driver := NewSQLiteDriver(db, "trifle_stats", JoinedFull)
	if err := driver.Setup(); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.TimeZone = "UTC"
	cfg.Granularities = []string{"1d"}

	at := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

	if err := Track(cfg, "event::logs", at, map[string]any{"count": 1}); err != nil {
		t.Fatalf("track failed: %v", err)
	}
	if err := Assert(cfg, "event::logs", at, map[string]any{"count": 5}); err != nil {
		t.Fatalf("assert failed: %v", err)
	}

	from := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 1, 15, 23, 59, 0, 0, time.UTC)
	result, err := Values(cfg, "event::logs", from, to, "1d", false)
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
