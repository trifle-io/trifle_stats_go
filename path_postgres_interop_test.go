package triflestats

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"testing"
	"time"
)

// Opt-in interoperability check. All languages increment the same isolated scope.
func TestPathPostgresInterop(t *testing.T) {
	scope := os.Getenv("TRIFLE_PATH_INTEROP_SCOPE")
	if scope == "" {
		t.Skip("TRIFLE_PATH_INTEROP_SCOPE not set")
	}
	if !regexp.MustCompile(`^paths_[a-z0-9_]+$`).MatchString(scope) {
		t.Fatal("scope must begin paths_ and contain lowercase letters, digits, or underscores")
	}
	count, err := strconv.Atoi(os.Getenv("TRIFLE_PATH_INTEROP_COUNT"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", os.Getenv("POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	driver := NewPostgresDriver(db, scope, JoinedFull)
	ctx := context.Background()
	if err := driver.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/stats-paths-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Cases []struct{ Input, Storage map[string]any } `json:"packing_cases"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	fixture := fixtures.Cases[0]
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	key := Key{Key: "cross.language::job", Granularity: "1h", At: &at}
	if os.Getenv("TRIFLE_PATH_INTEROP_WRITE") == "1" {
		if err := driver.Inc(ctx, []Key{key}, fixture.Input); err != nil {
			t.Fatal(err)
		}
	}
	expectedStorage := map[string]any{}
	for name, value := range fixture.Storage {
		expectedStorage[name] = value.(float64) * float64(count)
	}
	rows, err := driver.Get(ctx, []Key{key})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, []map[string]any{Unpack(expectedStorage)}) {
		t.Fatalf("logical values: %#v", rows)
	}
	var physicalJSON string
	if err := db.QueryRow("SELECT data FROM "+scope+" WHERE key = $1", key.Join("::")).Scan(&physicalJSON); err != nil {
		t.Fatal(err)
	}
	var physical map[string]any
	if err := json.Unmarshal([]byte(physicalJSON), &physical); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(physical, expectedStorage) {
		t.Fatalf("physical fields: %#v", physical)
	}
	system := key
	system.Key = systemKeyName
	systemRows, err := driver.Get(ctx, []Key{system})
	if err != nil {
		t.Fatal(err)
	}
	if got := systemRows[0]["keys"].(map[string]any)[key.Key]; got != float64(count) {
		t.Fatalf("metric identifier changed: %#v", systemRows)
	}
}
