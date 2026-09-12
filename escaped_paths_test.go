package triflestats

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestEscapedPathQueries(t *testing.T) {
	input := map[string]any{`status\.items`: []any{map[string]any{"test%2Erb": "%2E"}}}
	wantLeaf := map[string]any{"status.items": []any{map[string]any{"test%2Erb": "%2E"}}}
	if got := Unpack(Pack(input)); !reflect.DeepEqual(got, wantLeaf) {
		t.Fatalf("opaque array payload changed: %#v", got)
	}
	series := Series{At: []time.Time{time.Now()}, Values: []map[string]any{{"jobs": map[string]any{
		"test.rb": 2, "test": map[string]any{"rb": 3}, "*": 5, "test%2Erb": 7,
	}}}}
	for path, want := range map[string]float64{`jobs.test\.rb`: 2, `jobs.test.rb`: 3, `jobs.\*`: 5} {
		if got := series.FormatCategory(path, 1, nil); !reflect.DeepEqual(got, map[string]any{path: want}) {
			t.Fatalf("category %q: %#v", path, got)
		}
	}
	expanded := series.FormatCategory("jobs.*", 1, nil).(map[string]any)
	if expanded[`jobs.\*`] != 5.0 || expanded[`jobs.test\.rb`] != 2.0 {
		t.Fatalf("expanded: %#v", expanded)
	}
	for path := range expanded {
		if PathHasWildcard(path) {
			t.Fatalf("concrete path became wildcard: %q", path)
		}
	}
	if got := series.FormatTimeline(`jobs.\*`, 1, nil); len(got) != 1 || got[`jobs.\*`] == nil {
		t.Fatalf("timeline: %#v", got)
	}
	updated, err := series.TransformExpression([]string{`jobs.test\.rb`, `jobs.\*`}, "a * b", `result.\*\.rb`)
	if err != nil {
		t.Fatal(err)
	}
	if got := FetchPath(updated.Values[0], `result.\*\.rb`); got != 10.0 {
		t.Fatalf("expression: %#v", got)
	}
	for _, path := range []string{"jobs.*", "jobs.test*rb"} {
		if err := ValidateExpression([]string{path}, "a", "result"); err == nil {
			t.Fatalf("accepted wildcard %q", path)
		}
	}
	if err := ValidateExpression([]string{`jobs.\*`}, "a", "result.*"); err == nil {
		t.Fatal("accepted wildcard output")
	}
}

func TestEscapedPathLocalDrivers(t *testing.T) {
	factories := map[string]func(*testing.T) Driver{
		"process": func(t *testing.T) Driver { return NewProcessDriver() },
		"redis":   func(t *testing.T) Driver { d, _, _ := newMiniRedisDriver(t, "escaped"); return d },
	}
	for name, mode := range map[string]JoinedIdentifier{"full": JoinedFull, "partial": JoinedPartial, "separated": JoinedSeparated} {
		factories["sqlite_"+name] = func(t *testing.T) Driver {
			d := NewSQLiteDriver(newTestDB(t), "escaped_paths", mode)
			if err := d.Setup(context.Background()); err != nil {
				t.Fatal(err)
			}
			return d
		}
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) { exerciseEscapedPaths(t, factory(t)) })
	}
}

func TestEscapedPathServiceDrivers(t *testing.T) {
	for _, mode := range []JoinedIdentifier{JoinedFull, JoinedPartial, JoinedSeparated} {
		t.Run(fmt.Sprintf("postgres_%d", mode), func(t *testing.T) {
			dsn := os.Getenv("POSTGRES_DSN")
			if dsn == "" {
				t.Skip("POSTGRES_DSN not set")
			}
			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			table := fmt.Sprintf("test_paths_%d", time.Now().UnixNano())
			d := NewPostgresDriver(db, table, mode)
			if err := d.Setup(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer db.Exec("DROP TABLE IF EXISTS " + table)
			defer db.Exec("DROP TABLE IF EXISTS " + table + "_ping")
			exerciseEscapedPaths(t, d)
		})
		t.Run(fmt.Sprintf("mongo_%d", mode), func(t *testing.T) {
			db := integrationMongoDatabase(t)
			collection := db.Collection(fmt.Sprintf("test_paths_%d", time.Now().UnixNano()))
			defer collection.Drop(context.Background())
			d := NewMongoDriver(collection, mode)
			if err := d.Setup(context.Background()); err != nil {
				t.Fatal(err)
			}
			exerciseEscapedPaths(t, d)
		})
		t.Run(fmt.Sprintf("mysql_%d", mode), func(t *testing.T) {
			dsn := os.Getenv("MYSQL_DSN")
			if dsn == "" {
				t.Skip("MYSQL_DSN not set")
			}
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			table := fmt.Sprintf("test_paths_%d", time.Now().UnixNano())
			d := NewMySQLDriver(db, table, mode)
			if err := d.Setup(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer db.Exec("DROP TABLE IF EXISTS " + table)
			defer db.Exec("DROP TABLE IF EXISTS " + table + "_ping")
			exerciseEscapedPaths(t, d)
		})
	}
	t.Run("redis", func(t *testing.T) {
		client := integrationRedisClient(t)
		prefix := fmt.Sprintf("test_paths_%d", time.Now().UnixNano())
		cleanupRedisPrefix(t, client, prefix)
		exerciseEscapedPaths(t, NewRedisDriver(client, prefix))
	})
}

func exerciseEscapedPaths(t *testing.T, driver Driver) {
	t.Helper()
	data, err := os.ReadFile("testdata/stats-paths-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Cases []struct{ Input, Logical map[string]any } `json:"packing_cases"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	fixture := fixtures.Cases[0]
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	keys := []Key{{Key: `escaped.Trifle::Jobs\*%2E`, Granularity: "1h", At: &at}}
	if err := driver.Inc(ctx, keys, fixture.Input); err != nil {
		t.Fatal(err)
	}
	rows, err := driver.Get(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !reflect.DeepEqual(rows[0], fixture.Logical) {
		t.Fatalf("get: %#v != %#v", rows, fixture.Logical)
	}
	buffer := NewBuffer(driver, BufferOptions{Aggregate: true, Async: false, Size: 100})
	t.Cleanup(func() { _ = buffer.Shutdown() })
	if err := buffer.Inc(ctx, keys, map[string]any{`jobs.test\.rb`: 2, "jobs.test.rb": 3, "jobs.*": 5}); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Inc(ctx, keys, map[string]any{"jobs": map[string]any{`test\.rb`: 7, "test": map[string]any{"rb": 11}, `\*`: 13}}); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Flush(); err != nil {
		t.Fatal(err)
	}
	rows, err = driver.Get(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	want := cloneMap(fixture.Logical)
	jobs := want["jobs"].(map[string]any)
	jobs["test.rb"] = 11.0
	jobs["test"] = map[string]any{"rb": 15.0}
	jobs["*"] = 22.0
	if !reflect.DeepEqual(rows[0], want) {
		t.Fatalf("buffer: %#v != %#v", rows[0], want)
	}
	if err := driver.Set(ctx, keys, map[string]any{`jobs.test\.rb`: 50.0}); err != nil {
		t.Fatal(err)
	}
	rows, err = driver.Get(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	jobs["test.rb"] = 50.0
	if !reflect.DeepEqual(rows[0], want) {
		t.Fatalf("set: %#v != %#v", rows[0], want)
	}
	// Exercise native JSON-path handling for every encoded/quoted field on update,
	// not just insertion of a complete JSON object.
	for i := 0; i < 2; i++ {
		if err := driver.Set(ctx, keys, fixture.Input); err != nil {
			t.Fatal(err)
		}
	}
	if err := driver.Inc(ctx, keys, EscapePathKeys(fixture.Logical)); err != nil {
		t.Fatal(err)
	}
	wantPacked := Pack(fixture.Input)
	for name, value := range wantPacked {
		wantPacked[name] = toFloatDefault(value, 0) * 2
	}
	rows, err = driver.Get(ctx, keys)
	if err != nil || !reflect.DeepEqual(rows[0], Unpack(wantPacked)) {
		t.Fatalf("repeat update: %#v, %v", rows, err)
	}
	statusKey := Key{Key: "escaped_status", At: &at}
	if err := driver.Ping(ctx, statusKey, fixture.Input); err != nil {
		t.Fatal(err)
	}
	gotAt, status, found, err := driver.Scan(ctx, statusKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found { // Some drivers/modes deliberately do not support beam/scan.
		return
	}
	if data, ok := status["data"].(map[string]any); ok {
		status = data // SQL status results retain their existing envelope.
	}
	if !gotAt.Equal(at) || !reflect.DeepEqual(status, fixture.Logical) {
		t.Fatalf("beam/scan: %v %#v", gotAt, status)
	}
}
