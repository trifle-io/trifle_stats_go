package triflestats

import (
	"testing"
	"time"
)

func TestDefaultConfig_BufferDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.BufferEnabled {
		t.Fatalf("expected buffer enabled by default")
	}
	if cfg.BufferDuration != time.Second {
		t.Fatalf("expected default buffer duration %v, got %v", time.Second, cfg.BufferDuration)
	}
	if cfg.BufferSize != 256 {
		t.Fatalf("expected default buffer size 256, got %d", cfg.BufferSize)
	}
	if !cfg.BufferAggregate {
		t.Fatalf("expected buffer aggregate enabled by default")
	}
	if !cfg.BufferAsync {
		t.Fatalf("expected buffer async enabled by default")
	}
}

func TestConfig_EffectiveGranularitiesVariants(t *testing.T) {
	cfg := DefaultConfig()

	cfg.Granularities = nil
	if got := cfg.EffectiveGranularities(); len(got) != len(DefaultGranularities) {
		t.Fatalf("expected default granularities length %d, got %d", len(DefaultGranularities), len(got))
	}

	cfg.Granularities = []string{}
	if got := cfg.EffectiveGranularities(); len(got) != 0 {
		t.Fatalf("expected empty granularities, got %+v", got)
	}

	cfg.Granularities = []string{"1m", "1h", "1h", "invalid", "1d"}
	got := cfg.EffectiveGranularities()
	expect := []string{"1m", "1h", "1d"}
	if len(got) != len(expect) {
		t.Fatalf("unexpected granularity count: %+v", got)
	}
	for i := range got {
		if got[i] != expect[i] {
			t.Fatalf("unexpected granularity at %d: %s != %s", i, got[i], expect[i])
		}
	}
}

func TestConfig_StorageRespectsBufferEnabled(t *testing.T) {
	driver := newBufferTestDriver()
	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = false

	storage := cfg.Storage()
	if storage != driver {
		t.Fatalf("expected raw driver when buffer disabled")
	}
}

func TestConfig_StorageUsesBufferWhenEnabled(t *testing.T) {
	driver := newBufferTestDriver()
	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = true
	cfg.BufferDuration = 0
	cfg.BufferSize = 2
	cfg.BufferAggregate = false
	cfg.BufferAsync = false

	storage := cfg.Storage()
	if _, ok := storage.(*Buffer); !ok {
		t.Fatalf("expected buffered storage")
	}

	at := time.Date(2025, 2, 1, 10, 30, 0, 0, time.UTC)
	if err := Track(cfg, "events", at, map[string]any{"count": 1}); err != nil {
		t.Fatalf("first track failed: %v", err)
	}
	if err := Track(cfg, "events", at, map[string]any{"count": 2}); err != nil {
		t.Fatalf("second track failed: %v", err)
	}

	if err := cfg.FlushBuffer(); err != nil {
		t.Fatalf("flush buffer failed: %v", err)
	}
	if err := cfg.ShutdownBuffer(); err != nil {
		t.Fatalf("shutdown buffer failed: %v", err)
	}

	writes := driver.snapshot()
	if len(writes) == 0 {
		t.Fatalf("expected writes flushed through buffer")
	}
}

func TestConfig_BufferRebuildsWhenOptionsChange(t *testing.T) {
	driver := newBufferTestDriver()
	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = true
	cfg.BufferDuration = 50 * time.Millisecond
	cfg.BufferSize = 5
	cfg.BufferAggregate = true
	cfg.BufferAsync = true

	first := cfg.Storage()
	if first == nil {
		t.Fatalf("expected initial storage")
	}

	cfg.BufferSize = 10
	second := cfg.Storage()
	if second == nil {
		t.Fatalf("expected rebuilt storage")
	}
	if first == second {
		t.Fatalf("expected storage to rebuild after option change")
	}

	if err := cfg.ShutdownBuffer(); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
}

func TestConfig_LocationHandlesInvalidTimezone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TimeZone = "Invalid/Timezone"

	loc := cfg.Location()
	if loc != time.UTC {
		t.Fatalf("expected UTC fallback, got %v", loc)
	}
	if cfg.TimezoneLoadError == nil {
		t.Fatalf("expected timezone load error to be recorded")
	}
}

func TestConfig_LocationValidTimezone(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TimeZone = "UTC"

	loc := cfg.Location()
	if loc.String() != "UTC" {
		t.Fatalf("expected UTC location, got %s", loc.String())
	}
}
