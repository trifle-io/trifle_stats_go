package triflestats

import (
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteDriver_Full(t *testing.T) {
	runSQLiteDriverModeTest(t, JoinedFull)
}

func TestSQLiteDriver_Partial(t *testing.T) {
	runSQLiteDriverModeTest(t, JoinedPartial)
}

func TestSQLiteDriver_Separated(t *testing.T) {
	runSQLiteDriverModeTest(t, JoinedSeparated)
}

func runSQLiteDriverModeTest(t *testing.T, mode JoinedIdentifier) {
	db := newTestDB(t)
	driver := NewSQLiteDriver(db, "trifle_stats", mode)
	if err := driver.Setup(); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	at := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	key := Key{Key: "event", Granularity: "1d", At: &at}

	values := map[string]any{
		"count": 1,
		"meta": map[string]any{
			"duration": 2,
		},
	}

	if err := driver.Set([]Key{key}, values); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	got, err := driver.Get([]Key{key})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d", len(got))
	}
	expect := map[string]any{
		"count": float64(1),
		"meta": map[string]any{
			"duration": float64(2),
		},
	}
	if !reflect.DeepEqual(got[0], expect) {
		t.Fatalf("unexpected data: %+v", got[0])
	}

	// set should not delete other keys
	if err := driver.Set([]Key{key}, map[string]any{"count": 5}); err != nil {
		t.Fatalf("set update failed: %v", err)
	}
	got, err = driver.Get([]Key{key})
	if err != nil {
		t.Fatalf("get after set failed: %v", err)
	}
	expect = map[string]any{
		"count": float64(5),
		"meta": map[string]any{
			"duration": float64(2),
		},
	}
	if !reflect.DeepEqual(got[0], expect) {
		t.Fatalf("unexpected data after set: %+v", got[0])
	}

	// inc should add
	if err := driver.Inc([]Key{key}, map[string]any{"count": 2}); err != nil {
		t.Fatalf("inc failed: %v", err)
	}
	got, err = driver.Get([]Key{key})
	if err != nil {
		t.Fatalf("get after inc failed: %v", err)
	}
	expect = map[string]any{
		"count": float64(7),
		"meta": map[string]any{
			"duration": float64(2),
		},
	}
	if !reflect.DeepEqual(got[0], expect) {
		t.Fatalf("unexpected data after inc: %+v", got[0])
	}
}

func TestSQLiteDriver_SystemTracking(t *testing.T) {
	modes := []JoinedIdentifier{JoinedFull, JoinedPartial, JoinedSeparated}
	for _, mode := range modes {
		t.Run(modeName(mode), func(t *testing.T) {
			db := newTestDB(t)
			driver := NewSQLiteDriver(db, "trifle_stats", mode)
			if err := driver.Setup(); err != nil {
				t.Fatalf("setup failed: %v", err)
			}

			at := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
			key := Key{Key: "event::logs", Granularity: "1h", At: &at}

			if err := driver.Inc([]Key{key}, map[string]any{"count": 1}); err != nil {
				t.Fatalf("inc failed: %v", err)
			}

			systemKey := Key{Key: systemKeyName, Granularity: "1h", At: &at}
			got, err := driver.Get([]Key{systemKey})
			if err != nil {
				t.Fatalf("get system key failed: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("expected 1 system result, got %d", len(got))
			}
			expect := map[string]any{
				"count": float64(1),
				"keys": map[string]any{
					"event::logs": float64(1),
				},
			}
			if !reflect.DeepEqual(got[0], expect) {
				t.Fatalf("unexpected system data: %+v", got[0])
			}
		})
	}
}

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:memdb_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	return db
}

func modeName(mode JoinedIdentifier) string {
	switch mode {
	case JoinedFull:
		return "full"
	case JoinedPartial:
		return "partial"
	case JoinedSeparated:
		return "separated"
	default:
		return fmt.Sprintf("mode_%d", mode)
	}
}
