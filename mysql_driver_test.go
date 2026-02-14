package triflestats

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMySQLDriver_SetupCreatesModeSpecificSchema(t *testing.T) {
	tests := []struct {
		name    string
		mode    JoinedIdentifier
		pattern string
	}{
		{
			name:    "full",
			mode:    JoinedFull,
			pattern: "CREATE TABLE IF NOT EXISTS `test_stats` .*`key` VARCHAR\\(255\\) PRIMARY KEY",
		},
		{
			name:    "partial",
			mode:    JoinedPartial,
			pattern: "CREATE TABLE IF NOT EXISTS `test_stats` .*PRIMARY KEY \\(`key`, `at`\\)",
		},
		{
			name:    "separated",
			mode:    JoinedSeparated,
			pattern: "CREATE TABLE IF NOT EXISTS `test_stats` .*PRIMARY KEY \\(`key`, `granularity`, `at`\\)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock setup failed: %v", err)
			}
			defer db.Close()

			driver := NewMySQLDriver(db, "test_stats", tt.mode)
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

func TestMySQLDriver_Description(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	full := NewMySQLDriver(db, "stats", JoinedFull)
	if got := full.Description(); got != "MySQLDriver(J)" {
		t.Fatalf("unexpected full description: %s", got)
	}
	partial := NewMySQLDriver(db, "stats", JoinedPartial)
	if got := partial.Description(); got != "MySQLDriver(P)" {
		t.Fatalf("unexpected partial description: %s", got)
	}
	separated := NewMySQLDriver(db, "stats", JoinedSeparated)
	if got := separated.Description(); got != "MySQLDriver(S)" {
		t.Fatalf("unexpected separated description: %s", got)
	}
}

func TestMySQLDriver_RequiresAtForPartialAndSeparated(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	partial := NewMySQLDriver(db, "stats_partial", JoinedPartial)
	if _, err := partial.identifierForKey(Key{Key: "events", Granularity: "1h"}); err == nil {
		t.Fatalf("expected error when At is missing for partial mode")
	}

	separated := NewMySQLDriver(db, "stats_separated", JoinedSeparated)
	if _, err := separated.identifierForKey(Key{Key: "events", Granularity: "1h"}); err == nil {
		t.Fatalf("expected error when At is missing for separated mode")
	}
}

func TestMySQLDriver_SetIncGet_WithMockedDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewMySQLDriver(db, "test_stats", JoinedFull)
	driver.SystemTracking = false

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}
	joinedKey := key.Join("::")

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO `+"`test_stats`"+` (`+"`key`"+`, `+"`data`"+`) VALUES (?, CAST(? AS JSON)) ON DUPLICATE KEY UPDATE `+"`data`"+` = JSON_SET(COALESCE(`+"`data`"+`, JSON_OBJECT()), '$."count"', CAST(? AS JSON), '$."meta.duration"', CAST(? AS JSON));`,
	)).
		WithArgs(joinedKey, sqlmock.AnyArg(), "1", "2").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.Set([]Key{key}, map[string]any{"count": 1, "meta": map[string]any{"duration": 2}}); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO `+"`test_stats`"+` (`+"`key`"+`, `+"`data`"+`) VALUES (?, CAST(? AS JSON)) ON DUPLICATE KEY UPDATE `+"`data`"+` = JSON_SET(COALESCE(`+"`data`"+`, JSON_OBJECT()), '$."count"', (COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(`+"`data`"+`, JSON_OBJECT()), '$."count"')) AS DECIMAL(65,10)), 0) + CAST(? AS DECIMAL(65,10))));`,
	)).
		WithArgs(joinedKey, sqlmock.AnyArg(), float64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.Inc([]Key{key}, map[string]any{"count": 2}); err != nil {
		t.Fatalf("inc failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT `key`, CAST(`data` AS CHAR) AS data FROM `test_stats` WHERE (`key` = ?);")).
		WithArgs(joinedKey).
		WillReturnRows(sqlmock.NewRows([]string{"key", "data"}).AddRow(joinedKey, `{"count":3,"meta.duration":2}`))

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

func TestMySQLDriver_IncCountPropagatesSystemTrackingCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewMySQLDriver(db, "test_stats", JoinedSeparated)
	driver.SystemTracking = true

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{
		Key:         "events",
		TrackingKey: "__untracked__",
		Granularity: "1h",
		At:          &at,
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO `+"`test_stats`"+` (`+"`key`"+`, `+"`granularity`"+`, `+"`at`"+`, `+"`data`"+`) VALUES (?, ?, ?, CAST(? AS JSON)) ON DUPLICATE KEY UPDATE `+"`data`"+` = JSON_SET(COALESCE(`+"`data`"+`, JSON_OBJECT()), '$."count"', (COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(`+"`data`"+`, JSON_OBJECT()), '$."count"')) AS DECIMAL(65,10)), 0) + CAST(? AS DECIMAL(65,10))));`,
	)).
		WithArgs("events", "1h", formatMySQLAt(at), jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(2)
		}}, float64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO `+"`test_stats`"+` (`+"`key`"+`, `+"`granularity`"+`, `+"`at`"+`, `+"`data`"+`) VALUES (?, ?, ?, CAST(? AS JSON)) ON DUPLICATE KEY UPDATE `+"`data`"+` = JSON_SET(COALESCE(`+"`data`"+`, JSON_OBJECT()), '$."count"', (COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(`+"`data`"+`, JSON_OBJECT()), '$."count"')) AS DECIMAL(65,10)), 0) + CAST(? AS DECIMAL(65,10))), '$."keys.__untracked__"', (COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(`+"`data`"+`, JSON_OBJECT()), '$."keys.__untracked__"')) AS DECIMAL(65,10)), 0) + CAST(? AS DECIMAL(65,10))));`,
	)).
		WithArgs(systemKeyName, "1h", formatMySQLAt(at), jsonArgMatcher{validate: func(data map[string]any) bool {
			return data["count"] == float64(3) && data["keys.__untracked__"] == float64(3)
		}}, float64(3), float64(3)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := driver.IncCount([]Key{key}, map[string]any{"count": 2}, 3); err != nil {
		t.Fatalf("inc count failed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMySQLDriver_GetReturnsEmptyMapWhenMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock setup failed: %v", err)
	}
	defer db.Close()

	driver := NewMySQLDriver(db, "test_stats", JoinedFull)
	driver.SystemTracking = false

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}
	joinedKey := key.Join("::")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT `key`, CAST(`data` AS CHAR) AS data FROM `test_stats` WHERE (`key` = ?);")).
		WithArgs(joinedKey).
		WillReturnRows(sqlmock.NewRows([]string{"key", "data"}))

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

func TestMySQLDriver_IntegrationModes(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("MYSQL_DSN not set")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql failed: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("ping mysql failed: %v", err)
	}

	modes := []JoinedIdentifier{JoinedFull, JoinedPartial, JoinedSeparated}
	for _, mode := range modes {
		t.Run(modeName(mode), func(t *testing.T) {
			table := fmt.Sprintf("test_stats_go_mysql_%s_%d", modeName(mode), time.Now().UnixNano())
			driver := NewMySQLDriver(db, table, mode)
			driver.SystemTracking = true

			if err := driver.Setup(); err != nil {
				t.Fatalf("setup failed: %v", err)
			}
			defer func() {
				_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
			}()

			at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
			key := Key{Key: "events", Granularity: "1h", At: &at, TrackingKey: "manual"}

			if err := driver.Set([]Key{key}, map[string]any{"count": 1, "meta": map[string]any{"duration": 2}}); err != nil {
				t.Fatalf("set failed: %v", err)
			}
			if err := driver.IncCount([]Key{key}, map[string]any{"count": 2}, 3); err != nil {
				t.Fatalf("inc failed: %v", err)
			}

			values, err := driver.Get([]Key{key})
			if err != nil {
				t.Fatalf("get failed: %v", err)
			}
			if got := values[0]["count"]; got != float64(3) {
				t.Fatalf("expected count 3, got %#v", got)
			}
			meta := values[0]["meta"].(map[string]any)
			if got := meta["duration"]; got != float64(2) {
				t.Fatalf("expected meta.duration 2, got %#v", got)
			}

			systemKey := Key{Key: systemKeyName, Granularity: "1h", At: &at}
			systemValues, err := driver.Get([]Key{systemKey})
			if err != nil {
				t.Fatalf("get system failed: %v", err)
			}
			keysMap := systemValues[0]["keys"].(map[string]any)
			if got := keysMap["manual"]; got != float64(4) {
				t.Fatalf("expected system key manual=4, got %#v", got)
			}
		})
	}
}
