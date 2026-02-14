package triflestats

import (
	"database/sql"
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
		name    string
		mode    JoinedIdentifier
		pattern string
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
			name:    "separated",
			mode:    JoinedSeparated,
			pattern: "CREATE TABLE IF NOT EXISTS test_stats .*PRIMARY KEY \\(key, granularity, at\\)",
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

			if err := driver.Setup(); err != nil {
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

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT data FROM test_stats WHERE key = $1 LIMIT 1;")).
		WithArgs(joinedKey).
		WillReturnRows(sqlmock.NewRows([]string{"data"}))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO test_stats (key, data) VALUES ($1, $2::jsonb) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data;")).
		WithArgs(joinedKey, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(1) && data["meta.duration"] == float64(2)
		}}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.Set([]Key{key}, map[string]any{"count": 1, "meta": map[string]any{"duration": 2}}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT data FROM test_stats WHERE key = $1 LIMIT 1;")).
		WithArgs(joinedKey).
		WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow(`{"count":1,"meta.duration":2}`))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO test_stats (key, data) VALUES ($1, $2::jsonb) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data;")).
		WithArgs(joinedKey, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(3) && data["meta.duration"] == float64(2)
		}}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.Inc([]Key{key}, map[string]any{"count": 2}); err != nil {
		t.Fatalf("inc failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT data FROM test_stats WHERE key = $1 LIMIT 1;")).
		WithArgs(joinedKey).
		WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow(`{"count":3,"meta.duration":2}`))

	values, err := driver.Get([]Key{key})
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

	mock.ExpectBegin()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT data FROM test_stats WHERE key = $1 AND granularity = $2 AND at = $3 LIMIT 1;")).
		WithArgs("events", "1h", at).
		WillReturnRows(sqlmock.NewRows([]string{"data"}))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO test_stats (key, granularity, at, data) VALUES ($1, $2, $3, $4::jsonb) ON CONFLICT (key, granularity, at) DO UPDATE SET data = EXCLUDED.data;")).
		WithArgs("events", "1h", at, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(2)
		}}).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT data FROM test_stats WHERE key = $1 AND granularity = $2 AND at = $3 LIMIT 1;")).
		WithArgs(systemKeyName, "1h", at).
		WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow(`{"count":2,"keys.__untracked__":2}`))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO test_stats (key, granularity, at, data) VALUES ($1, $2, $3, $4::jsonb) ON CONFLICT (key, granularity, at) DO UPDATE SET data = EXCLUDED.data;")).
		WithArgs(systemKeyName, "1h", at, jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(5) && data["keys.__untracked__"] == float64(5)
		}}).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	if err := driver.IncCount([]Key{key}, map[string]any{"count": 2}, 3); err != nil {
		t.Fatalf("inc count failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
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

	mock.ExpectQuery(regexp.QuoteMeta("SELECT data FROM test_stats WHERE key = $1 LIMIT 1;")).
		WithArgs(joinedKey).
		WillReturnRows(sqlmock.NewRows([]string{"data"}))

	values, err := driver.Get([]Key{key})
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

func TestMergePackedValues(t *testing.T) {
	current := map[string]any{"count": float64(2), "meta.duration": float64(1)}
	incoming := map[string]any{"count": 3, "meta.duration": 2}

	inc, err := mergePackedValues(current, incoming, "inc")
	if err != nil {
		t.Fatalf("inc merge failed: %v", err)
	}
	if inc["count"] != float64(5) || inc["meta.duration"] != float64(3) {
		t.Fatalf("unexpected inc merge result: %+v", inc)
	}

	set, err := mergePackedValues(current, map[string]any{"status": "ok"}, "set")
	if err != nil {
		t.Fatalf("set merge failed: %v", err)
	}
	if set["status"] != "ok" || set["count"] != float64(2) {
		t.Fatalf("unexpected set merge result: %+v", set)
	}

	if _, err := mergePackedValues(current, map[string]any{"count": "invalid"}, "inc"); err == nil {
		t.Fatalf("expected error for non-numeric increment")
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

func TestPostgresDriver_ReadPackedNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewPostgresDriver(db, "test_stats", JoinedFull)
	key := Key{Key: "events", Granularity: "1h", At: ptrTime(time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC))}
	ident, err := driver.identifierForKey(key)
	if err != nil {
		t.Fatalf("identifier failed: %v", err)
	}

	query, args := driver.selectQuery(ident)
	sqlArgs := make([]sqldriver.Value, 0, len(args))
	for _, arg := range args {
		sqlArgs = append(sqlArgs, arg)
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(sqlArgs...).WillReturnError(sql.ErrNoRows)

	packed, err := driver.readPacked(nil, ident)
	if err != nil {
		t.Fatalf("read packed failed: %v", err)
	}
	if len(packed) != 0 {
		t.Fatalf("expected empty packed map, got %+v", packed)
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
