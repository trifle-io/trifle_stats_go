package triflestats

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newMiniRedisDriver(t *testing.T, prefix string) (*RedisDriver, *miniredis.Miniredis, *redis.Client) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis failed: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	driver := NewRedisDriver(client, prefix)

	t.Cleanup(func() {
		_ = client.Close()
		server.Close()
	})
	return driver, server, client
}

func TestRedisDriver_IncSetGetAndSystemTracking(t *testing.T) {
	driver, _, _ := newMiniRedisDriver(t, "test")
	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}

	if err := driver.SetCount([]Key{key}, map[string]any{"count": 1, "meta": map[string]any{"duration": 2}}, 2); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := driver.Inc([]Key{key}, map[string]any{"count": 3}); err != nil {
		t.Fatalf("inc failed: %v", err)
	}
	if err := driver.Set([]Key{key}, map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("set merge failed: %v", err)
	}

	values, err := driver.Get([]Key{key})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("expected one value map, got %d", len(values))
	}
	row := values[0]
	if got := row["count"]; got != 4.0 {
		t.Fatalf("expected count 4, got %#v", got)
	}
	meta := row["meta"].(map[string]any)
	if got := meta["duration"]; got != 2.0 {
		t.Fatalf("expected meta.duration 2, got %#v", got)
	}
	if got := row["status"]; got != "ok" {
		t.Fatalf("expected status ok, got %#v", got)
	}

	systemKey := Key{Key: systemKeyName, Granularity: "1h", At: &at}
	systemValues, err := driver.Get([]Key{systemKey})
	if err != nil {
		t.Fatalf("get system key failed: %v", err)
	}
	system := systemValues[0]
	if got := system["count"]; got != 4.0 {
		t.Fatalf("expected system count 4, got %#v", got)
	}
	keys := system["keys"].(map[string]any)
	if got := keys["events"]; got != 4.0 {
		t.Fatalf("expected system keys.events 4, got %#v", got)
	}
}

func TestRedisDriver_TrackingKeyOverride(t *testing.T) {
	driver, _, _ := newMiniRedisDriver(t, "test")
	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{
		Key:         "events",
		TrackingKey: "__untracked__",
		Granularity: "1h",
		At:          &at,
	}

	if err := driver.IncCount([]Key{key}, map[string]any{"count": 1}, 3); err != nil {
		t.Fatalf("inc count failed: %v", err)
	}

	systemKey := Key{Key: systemKeyName, Granularity: "1h", At: &at}
	systemValues, err := driver.Get([]Key{systemKey})
	if err != nil {
		t.Fatalf("get system key failed: %v", err)
	}
	keys := systemValues[0]["keys"].(map[string]any)
	if got := keys["__untracked__"]; got != 3.0 {
		t.Fatalf("expected untracked system count 3, got %#v", got)
	}
}

func TestRedisDriver_IncRejectsNonNumericValues(t *testing.T) {
	driver, _, _ := newMiniRedisDriver(t, "test")
	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}

	err := driver.Inc([]Key{key}, map[string]any{"status": "running"})
	if err == nil {
		t.Fatalf("expected error for non-numeric increment")
	}
}

func TestRedisDriver_UsesConfiguredSeparatorAndPrefix(t *testing.T) {
	driver, server, _ := newMiniRedisDriver(t, "custom")
	driver.Separator = "--"

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}

	if err := driver.Set([]Key{key}, map[string]any{"count": 1}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	expectedKey := Key{Prefix: "custom", Key: "events", Granularity: "1h", At: &at}.Join("--")
	if !server.Exists(expectedKey) {
		t.Fatalf("expected redis key %q to exist", expectedKey)
	}
}

func TestRedisDriver_Description(t *testing.T) {
	driver, _, _ := newMiniRedisDriver(t, "custom")
	if got := driver.Description(); got != "RedisDriver(J)" {
		t.Fatalf("unexpected description: %s", got)
	}
}
