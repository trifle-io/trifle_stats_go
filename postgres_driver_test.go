package triflestats

import (
	"context"
	sqldriver "database/sql/driver"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type jsonArgMatcher struct {
	validate func(map[string]any) bool
}

func (m jsonArgMatcher) Match(value sqldriver.Value) bool {
	var raw []byte
	switch node := value.(type) {
	case string:
		raw = []byte(node)
	case []byte:
		raw = node
	default:
		return false
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return false
	}
	return m.validate(decoded)
}

func TestPostgresDriver_SetupCreatesModeSpecificSchema(t *testing.T) {
	tests := []struct {
		name      string
		mode      JoinedIdentifier
		pattern   string
		pingTable bool
	}{
		{
			name:    "full",
			mode:    JoinedFull,
			pattern: "CREATE TABLE IF NOT EXISTS test_stats .*key VARCHAR\\(255\\) PRIMARY KEY",
		},
		{
			name:    "partial",
			mode:    JoinedPartial,
			pattern: "CREATE TABLE IF NOT EXISTS test_stats .*PRIMARY KEY \\(key, at\\)",
		},
		{
			name:      "separated",
			mode:      JoinedSeparated,
			pattern:   "CREATE TABLE IF NOT EXISTS test_stats .*PRIMARY KEY \\(key, granularity, at\\)",
			pingTable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock setup failed: %v", err)
			}
			defer db.Close()

			driver := NewPostgresDriver(db, "test_stats", tt.mode)
			mock.ExpectExec(tt.pattern).WillReturnResult(sqlmock.NewResult(0, 0))
			if tt.pingTable {
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS test_stats_ping .*key VARCHAR\\(255\\) PRIMARY KEY").
					WillReturnResult(sqlmock.NewResult(0, 0))
			}

			if err := driver.Setup(context.Background()); err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func TestPostgresDriver_IdentifierModes(t *testing.T) {
	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	fullDriver := NewPostgresDriver(db, "stats_full", JoinedFull)
	full, err := fullDriver.identifierForKey(key)
	if err != nil {
		t.Fatalf("full identifier failed: %v", err)
	}
	if len(full.columns) != 1 || full.columns[0] != "key" {
		t.Fatalf("unexpected full columns: %+v", full.columns)
	}

	partialDriver := NewPostgresDriver(db, "stats_partial", JoinedPartial)
	partial, err := partialDriver.identifierForKey(key)
	if err != nil {
		t.Fatalf("partial identifier failed: %v", err)
	}
	if len(partial.columns) != 2 || partial.columns[0] != "key" || partial.columns[1] != "at" {
		t.Fatalf("unexpected partial columns: %+v", partial.columns)
	}

	separatedDriver := NewPostgresDriver(db, "stats_separated", JoinedSeparated)
	separated, err := separatedDriver.identifierForKey(key)
	if err != nil {
		t.Fatalf("separated identifier failed: %v", err)
	}
	if len(separated.columns) != 3 || separated.columns[0] != "key" || separated.columns[1] != "granularity" || separated.columns[2] != "at" {
		t.Fatalf("unexpected separated columns: %+v", separated.columns)
	}
}

func TestPostgresDriver_Description(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	full := NewPostgresDriver(db, "stats", JoinedFull)
	if got := full.Description(); got != "PostgresDriver(J)" {
		t.Fatalf("unexpected full description: %s", got)
	}
	partial := NewPostgresDriver(db, "stats", JoinedPartial)
	if got := partial.Description(); got != "PostgresDriver(P)" {
		t.Fatalf("unexpected partial description: %s", got)
	}
	separated := NewPostgresDriver(db, "stats", JoinedSeparated)
	if got := separated.Description(); got != "PostgresDriver(S)" {
		t.Fatalf("unexpected separated description: %s", got)
	}
}

func TestPostgresDriver_SetIncGet_WithMockedDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewPostgresDriver(db, "test_stats", JoinedFull)
	driver.SystemTracking = false

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}
	joinedKey := key.Join("::")

	setQuery := "INSERT INTO test_stats (key, data) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET data = " +
		"jsonb_set(jsonb_set(to_jsonb(test_stats.data), '{count}', $3::jsonb), '{meta.duration}', $4::jsonb);"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(setQuery)).
		WithArgs(joinedKey, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(1) && data["meta.duration"] == float64(2)
		}}, "1", "2").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.Set(context.Background(), []Key{key}, map[string]any{"count": 1, "meta": map[string]any{"duration": 2}}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	incQuery := "INSERT INTO test_stats (key, data) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET data = " +
		"jsonb_set(to_jsonb(test_stats.data), '{count}', (COALESCE(test_stats.data->>'count', '0')::numeric + 2)::text::jsonb);"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(incQuery)).
		WithArgs(joinedKey, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(2)
		}}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.Inc(context.Background(), []Key{key}, map[string]any{"count": 2}); err != nil {
		t.Fatalf("inc failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT key, data FROM test_stats WHERE (key = $1);")).
		WithArgs(joinedKey).
		WillReturnRows(sqlmock.NewRows([]string{"key", "data"}).AddRow(joinedKey, `{"count":3,"meta.duration":2}`))

	values, err := driver.Get(context.Background(), []Key{key})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("expected one row, got %d", len(values))
	}
	if got := values[0]["count"]; got != float64(3) {
		t.Fatalf("expected count 3, got %#v", got)
	}
	meta := values[0]["meta"].(map[string]any)
	if got := meta["duration"]; got != float64(2) {
		t.Fatalf("expected meta.duration 2, got %#v", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresDriver_IncCountPropagatesSystemTrackingCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewPostgresDriver(db, "test_stats", JoinedSeparated)
	driver.SystemTracking = true

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{
		Key:         "events",
		TrackingKey: "__untracked__",
		Granularity: "1h",
		At:          &at,
	}

	mainQuery := "INSERT INTO test_stats (key, granularity, at, data) VALUES ($1, $2, $3, $4) ON CONFLICT (key, granularity, at) DO UPDATE SET data = " +
		"jsonb_set(to_jsonb(test_stats.data), '{count}', (COALESCE(test_stats.data->>'count', '0')::numeric + 2)::text::jsonb);"
	systemQuery := "INSERT INTO test_stats (key, granularity, at, data) VALUES ($1, $2, $3, $4) ON CONFLICT (key, granularity, at) DO UPDATE SET data = " +
		"jsonb_set(jsonb_set(to_jsonb(test_stats.data), '{count}', (COALESCE(test_stats.data->>'count', '0')::numeric + 3)::text::jsonb), " +
		"'{keys.__untracked__}', (COALESCE(test_stats.data->>'keys.__untracked__', '0')::numeric + 3)::text::jsonb);"

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(mainQuery)).
		WithArgs("events", "1h", at, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(2)
		}}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(systemQuery)).
		WithArgs(systemKeyName, "1h", at, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(3) && data["keys.__untracked__"] == float64(3)
		}}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.IncCount(context.Background(), []Key{key}, map[string]any{"count": 2}, 3); err != nil {
		t.Fatalf("inc count failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresDriver_IncRejectsNonNumericValues(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewPostgresDriver(db, "test_stats", JoinedFull)
	ident, err := driver.identifierForKey(Key{Key: "events", Granularity: "1h"})
	if err != nil {
		t.Fatalf("identifier failed: %v", err)
	}
	if _, _, err := driver.buildUpsertQuery(ident, map[string]any{"status": "running"}, "inc"); err == nil {
		t.Fatalf("expected error for non-numeric increment")
	}
}

func TestPostgresDriver_GetReturnsEmptyMapWhenMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewPostgresDriver(db, "test_stats", JoinedFull)
	driver.SystemTracking = false

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}
	joinedKey := key.Join("::")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT key, data FROM test_stats WHERE (key = $1);")).
		WithArgs(joinedKey).
		WillReturnRows(sqlmock.NewRows([]string{"key", "data"}))

	values, err := driver.Get(context.Background(), []Key{key})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("expected one row, got %d", len(values))
	}
	if len(values[0]) != 0 {
		t.Fatalf("expected empty map for missing row, got %+v", values[0])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresDriver_BulkGetUsesSingleQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewPostgresDriver(db, "test_stats", JoinedSeparated)

	at1 := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	at2 := at1.Add(time.Hour)
	key1 := Key{Key: "events", Granularity: "1h", At: &at1}
	key2 := Key{Key: "events", Granularity: "1h", At: &at2}

	query := "SELECT key, granularity, at, data FROM test_stats WHERE " +
		"(key = $1 AND granularity = $2 AND at = $3) OR (key = $4 AND granularity = $5 AND at = $6);"
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("events", "1h", at1, "events", "1h", at2).
		WillReturnRows(sqlmock.NewRows([]string{"key", "granularity", "at", "data"}).
			AddRow("events", "1h", at2, `{"count":7}`))

	values, err := driver.Get(context.Background(), []Key{key1, key2})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("expected two rows, got %d", len(values))
	}
	if len(values[0]) != 0 {
		t.Fatalf("expected first row empty, got %+v", values[0])
	}
	if got := values[1]["count"]; got != float64(7) {
		t.Fatalf("expected second row count 7, got %#v", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresDriver_PingAndScanQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewPostgresDriver(db, "test_stats", JoinedSeparated)

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "jobs::process", At: &at}

	pingQuery := "INSERT INTO test_stats_ping (key, at, data) VALUES ($1, $2, $3::jsonb) ON CONFLICT (key) DO UPDATE SET at = $2, data = $3::jsonb;"
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(pingQuery)).
		WithArgs("jobs::process", at, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["data.state"] == "ok" && data["at"] == float64(at.Unix())
		}}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.Ping(context.Background(), key, map[string]any{"state": "ok"}); err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	scanQuery := "SELECT at, data FROM test_stats_ping WHERE key = $1 ORDER BY at DESC LIMIT 1;"
	mock.ExpectQuery(regexp.QuoteMeta(scanQuery)).
		WithArgs("jobs::process").
		WillReturnRows(sqlmock.NewRows([]string{"at", "data"}).
			AddRow(at, `{"data.state":"ok","at":1738407600}`))

	scanAt, values, found, err := driver.Scan(context.Background(), Key{Key: "jobs::process"})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if !found {
		t.Fatalf("expected scan to find status")
	}
	if !scanAt.Equal(at) {
		t.Fatalf("expected scan at %v, got %v", at, scanAt)
	}
	data := values["data"].(map[string]any)
	if data["state"] != "ok" {
		t.Fatalf("unexpected scan values: %+v", values)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresDriver_PingScanNoOpInJoinedModes(t *testing.T) {
	driver := NewPostgresDriver(nil, "test_stats", JoinedFull)

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	if err := driver.Ping(context.Background(), Key{Key: "jobs", At: &at}, map[string]any{"state": "ok"}); err != nil {
		t.Fatalf("expected ping no-op, got %v", err)
	}
	_, _, found, err := driver.Scan(context.Background(), Key{Key: "jobs"})
	if err != nil {
		t.Fatalf("expected scan no-op, got %v", err)
	}
	if found {
		t.Fatalf("expected scan to report no status")
	}
}

func TestPostgresDriver_RequiresAtForPartialAndSeparated(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	partial := NewPostgresDriver(db, "stats_partial", JoinedPartial)
	if _, err := partial.identifierForKey(Key{Key: "events", Granularity: "1h"}); err == nil {
		t.Fatalf("expected error when At is missing for partial mode")
	}

	separated := NewPostgresDriver(db, "stats_separated", JoinedSeparated)
	if _, err := separated.identifierForKey(Key{Key: "events", Granularity: "1h"}); err == nil {
		t.Fatalf("expected error when At is missing for separated mode")
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
