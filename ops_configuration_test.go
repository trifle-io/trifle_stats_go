package triflestats

import (
	"testing"
	"time"
)

func TestTrackRespectsConfiguredGranularities(t *testing.T) {
	db := newTestDB(t)
	driver := NewSQLiteDriver(db, "trifle_stats", JoinedFull)
	if err := driver.Setup(); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = false
	cfg.TimeZone = "UTC"
	cfg.Granularities = []string{"1h", "1d", "invalid", "1h"}

	at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
	if err := Track(cfg, "events", at, map[string]any{"count": 1}); err != nil {
		t.Fatalf("track failed: %v", err)
	}

	hourAt := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	dayAt := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	minuteAt := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)

	values, err := driver.Get([]Key{
		{Key: "events", Granularity: "1h", At: &hourAt},
		{Key: "events", Granularity: "1d", At: &dayAt},
		{Key: "events", Granularity: "1m", At: &minuteAt},
	})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got := values[0]["count"]; got != float64(1) {
		t.Fatalf("expected 1h value, got %#v", got)
	}
	if got := values[1]["count"]; got != float64(1) {
		t.Fatalf("expected 1d value, got %#v", got)
	}
	if len(values[2]) != 0 {
		t.Fatalf("expected no write for non-configured granularity, got %+v", values[2])
	}
}

func TestTrack_BufferEnabledAndDisabledModes(t *testing.T) {
	at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)

	t.Run("buffer enabled", func(t *testing.T) {
		db := newTestDB(t)
		driver := NewSQLiteDriver(db, "trifle_stats", JoinedFull)
		if err := driver.Setup(); err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		cfg := DefaultConfig()
		cfg.Driver = driver
		cfg.TimeZone = "UTC"
		cfg.Granularities = []string{"1h"}
		cfg.BufferEnabled = true
		cfg.BufferAggregate = false
		cfg.BufferSize = 2
		cfg.BufferDuration = 0
		cfg.BufferAsync = false

		if err := Track(cfg, "events", at, map[string]any{"count": 1}); err != nil {
			t.Fatalf("first track failed: %v", err)
		}

		from := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
		to := time.Date(2025, 2, 1, 11, 59, 59, 0, time.UTC)
		before, err := Values(cfg, "events", from, to, "1h", false)
		if err != nil {
			t.Fatalf("values before flush failed: %v", err)
		}
		if len(before.Values) != 1 || len(before.Values[0]) != 0 {
			t.Fatalf("expected buffered write not yet visible, got %+v", before.Values)
		}

		if err := Track(cfg, "events", at.Add(10*time.Minute), map[string]any{"count": 1}); err != nil {
			t.Fatalf("second track failed: %v", err)
		}

		after, err := Values(cfg, "events", from, to, "1h", false)
		if err != nil {
			t.Fatalf("values after flush failed: %v", err)
		}
		if got := after.Values[0]["count"]; got != float64(2) {
			t.Fatalf("expected count 2 after flush, got %#v", got)
		}

		if err := cfg.ShutdownBuffer(); err != nil {
			t.Fatalf("shutdown buffer failed: %v", err)
		}
	})

	t.Run("buffer disabled", func(t *testing.T) {
		db := newTestDB(t)
		driver := NewSQLiteDriver(db, "trifle_stats", JoinedFull)
		if err := driver.Setup(); err != nil {
			t.Fatalf("setup failed: %v", err)
		}

		cfg := DefaultConfig()
		cfg.Driver = driver
		cfg.TimeZone = "UTC"
		cfg.Granularities = []string{"1h"}
		cfg.BufferEnabled = false

		if err := Track(cfg, "events", at, map[string]any{"count": 1}); err != nil {
			t.Fatalf("track failed: %v", err)
		}

		from := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
		to := time.Date(2025, 2, 1, 11, 59, 59, 0, time.UTC)
		result, err := Values(cfg, "events", from, to, "1h", false)
		if err != nil {
			t.Fatalf("values failed: %v", err)
		}
		if got := result.Values[0]["count"]; got != float64(1) {
			t.Fatalf("expected immediate count 1, got %#v", got)
		}
	})
}

func TestTrack_UntrackedUsesSharedSystemTrackingKey(t *testing.T) {
	db := newTestDB(t)
	driver := NewSQLiteDriver(db, "trifle_stats", JoinedFull)
	if err := driver.Setup(); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = false
	cfg.TimeZone = "UTC"
	cfg.Granularities = []string{"1h"}

	at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
	if err := Track(cfg, "events", at, map[string]any{"count": 1}, Untracked()); err != nil {
		t.Fatalf("track failed: %v", err)
	}

	bucket := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	systemValues, err := driver.Get([]Key{{Key: systemKeyName, Granularity: "1h", At: &bucket}})
	if err != nil {
		t.Fatalf("get system values failed: %v", err)
	}
	keys := systemValues[0]["keys"].(map[string]any)
	if got := keys[untrackedKeyName]; got != float64(1) {
		t.Fatalf("expected untracked counter 1, got %#v", got)
	}
}

func TestTrackAndValues_WorkAcrossIdentifierModes(t *testing.T) {
	modes := []JoinedIdentifier{JoinedFull, JoinedPartial, JoinedSeparated}

	for _, mode := range modes {
		t.Run(modeName(mode), func(t *testing.T) {
			db := newTestDB(t)
			driver := NewSQLiteDriver(db, "trifle_stats", mode)
			if err := driver.Setup(); err != nil {
				t.Fatalf("setup failed: %v", err)
			}

			cfg := DefaultConfig()
			cfg.Driver = driver
			cfg.BufferEnabled = false
			cfg.TimeZone = "UTC"
			cfg.Granularities = []string{"1h"}

			at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
			if err := Track(cfg, "events", at, map[string]any{"count": 2}); err != nil {
				t.Fatalf("track failed: %v", err)
			}
			if err := Assert(cfg, "events", at, map[string]any{"status": "ok"}); err != nil {
				t.Fatalf("assert failed: %v", err)
			}

			from := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
			to := time.Date(2025, 2, 1, 11, 59, 59, 0, time.UTC)
			result, err := Values(cfg, "events", from, to, "1h", false)
			if err != nil {
				t.Fatalf("values failed: %v", err)
			}
			if got := result.Values[0]["count"]; got != float64(2) {
				t.Fatalf("expected count 2, got %#v", got)
			}
			if got := result.Values[0]["status"]; got != "ok" {
				t.Fatalf("expected status ok, got %#v", got)
			}
		})
	}
}

func TestValues_SkipBlanksAndInvalidGranularity(t *testing.T) {
	db := newTestDB(t)
	driver := NewSQLiteDriver(db, "trifle_stats", JoinedFull)
	if err := driver.Setup(); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = false
	cfg.TimeZone = "UTC"
	cfg.Granularities = []string{"1h"}

	at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
	if err := Track(cfg, "events", at, map[string]any{"count": 1}); err != nil {
		t.Fatalf("track failed: %v", err)
	}

	from := time.Date(2025, 2, 1, 10, 0, 0, 0, time.UTC)
	to := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)

	withBlanks, err := Values(cfg, "events", from, to, "1h", false)
	if err != nil {
		t.Fatalf("values with blanks failed: %v", err)
	}
	if len(withBlanks.Values) != 2 {
		t.Fatalf("expected 2 buckets including blank, got %d", len(withBlanks.Values))
	}

	skipBlanks, err := Values(cfg, "events", from, to, "1h", true)
	if err != nil {
		t.Fatalf("values skip blanks failed: %v", err)
	}
	if len(skipBlanks.Values) != 1 {
		t.Fatalf("expected 1 non-blank bucket, got %d", len(skipBlanks.Values))
	}
	if got := skipBlanks.Values[0]["count"]; got != float64(1) {
		t.Fatalf("expected count 1 in non-blank bucket, got %#v", got)
	}

	if _, err := Values(cfg, "events", from, to, "invalid", false); err == nil {
		t.Fatalf("expected invalid granularity error")
	}
}
