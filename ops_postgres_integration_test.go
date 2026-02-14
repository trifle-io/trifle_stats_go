package triflestats

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestOpsWithPostgres_BufferEnabledAndDisabled(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres failed: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres failed: %v", err)
	}

	at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
	table := fmt.Sprintf("test_stats_go_pg_ops_%d", time.Now().UnixNano())
	driver := NewPostgresDriver(db, table, JoinedFull)
	if err := driver.Setup(); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	}()

	t.Run("buffer enabled", func(t *testing.T) {
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
		cfg := DefaultConfig()
		cfg.Driver = driver
		cfg.TimeZone = "UTC"
		cfg.Granularities = []string{"1h"}
		cfg.BufferEnabled = false

		if err := Track(cfg, "events_immediate", at, map[string]any{"count": 1}); err != nil {
			t.Fatalf("track failed: %v", err)
		}

		from := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
		to := time.Date(2025, 2, 1, 11, 59, 59, 0, time.UTC)
		result, err := Values(cfg, "events_immediate", from, to, "1h", false)
		if err != nil {
			t.Fatalf("values failed: %v", err)
		}
		if got := result.Values[0]["count"]; got != float64(1) {
			t.Fatalf("expected immediate count 1, got %#v", got)
		}
	})
}

func TestOpsWithPostgres_IdentifierModesAndGranularityFiltering(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres failed: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres failed: %v", err)
	}

	at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
	modes := []JoinedIdentifier{JoinedFull, JoinedPartial, JoinedSeparated}

	for _, mode := range modes {
		t.Run(modeName(mode), func(t *testing.T) {
			table := fmt.Sprintf("test_stats_go_pg_modes_%s_%d", modeName(mode), time.Now().UnixNano())
			driver := NewPostgresDriver(db, table, mode)
			if err := driver.Setup(); err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			defer func() {
				_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
			}()

			cfg := DefaultConfig()
			cfg.Driver = driver
			cfg.BufferEnabled = false
			cfg.TimeZone = "UTC"
			cfg.Granularities = []string{"1h", "1d", "invalid", "1h"}

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

			dayFrom := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
			dayTo := time.Date(2025, 2, 1, 23, 59, 59, 0, time.UTC)
			dayResult, err := Values(cfg, "events", dayFrom, dayTo, "1d", false)
			if err != nil {
				t.Fatalf("day values failed: %v", err)
			}
			if got := dayResult.Values[0]["count"]; got != float64(2) {
				t.Fatalf("expected 1d count 2, got %#v", got)
			}
		})
	}
}
