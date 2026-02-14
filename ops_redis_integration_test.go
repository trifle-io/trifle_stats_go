package triflestats

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestOpsWithRedis_BufferEnabledAndDisabled(t *testing.T) {
	client := integrationRedisClient(t)

	t.Run("buffer enabled", func(t *testing.T) {
		prefix := fmt.Sprintf("test_stats_go_redis_buf_%d", time.Now().UnixNano())
		driver := NewRedisDriver(client, prefix)
		cleanupRedisPrefix(t, client, prefix)

		cfg := DefaultConfig()
		cfg.Driver = driver
		cfg.TimeZone = "UTC"
		cfg.Granularities = []string{"1h"}
		cfg.BufferEnabled = true
		cfg.BufferAggregate = false
		cfg.BufferSize = 2
		cfg.BufferDuration = 0
		cfg.BufferAsync = false

		at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
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
		prefix := fmt.Sprintf("test_stats_go_redis_now_%d", time.Now().UnixNano())
		driver := NewRedisDriver(client, prefix)
		cleanupRedisPrefix(t, client, prefix)

		cfg := DefaultConfig()
		cfg.Driver = driver
		cfg.TimeZone = "UTC"
		cfg.Granularities = []string{"1h"}
		cfg.BufferEnabled = false

		at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
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

func TestOpsWithRedis_GranularityFilteringAndUntracked(t *testing.T) {
	client := integrationRedisClient(t)
	prefix := fmt.Sprintf("test_stats_go_redis_modes_%d", time.Now().UnixNano())
	driver := NewRedisDriver(client, prefix)
	cleanupRedisPrefix(t, client, prefix)

	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = false
	cfg.TimeZone = "UTC"
	cfg.Granularities = []string{"1h", "1d", "invalid", "1h"}

	at := time.Date(2025, 2, 1, 11, 35, 0, 0, time.UTC)
	if err := Track(cfg, "events", at, map[string]any{"count": 1}, Untracked()); err != nil {
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
	if got := result.Values[0]["count"]; got != float64(1) {
		t.Fatalf("expected 1h count 1, got %#v", got)
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
	if got := dayResult.Values[0]["count"]; got != float64(1) {
		t.Fatalf("expected 1d count 1, got %#v", got)
	}

	hourBucket := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	systemValues, err := driver.Get([]Key{{Key: systemKeyName, Granularity: "1h", At: &hourBucket}})
	if err != nil {
		t.Fatalf("get system values failed: %v", err)
	}
	keysMap, ok := systemValues[0]["keys"].(map[string]any)
	if !ok {
		t.Fatalf("expected system keys map, got %#v", systemValues[0]["keys"])
	}
	if got := keysMap[untrackedKeyName]; got != float64(1) {
		t.Fatalf("expected untracked system key count 1, got %#v", got)
	}
}

func integrationRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("ping redis failed: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func cleanupRedisPrefix(t *testing.T, client *redis.Client, prefix string) {
	t.Helper()

	ctx := context.Background()
	iter := client.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		if err := client.Del(ctx, iter.Val()).Err(); err != nil {
			t.Fatalf("redis cleanup failed: %v", err)
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("redis scan cleanup failed: %v", err)
	}
}
