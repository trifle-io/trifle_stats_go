package triflestats

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteDriver implements the Driver interface using SQLite JSON1.
type SQLiteDriver struct {
	DB               *sql.DB
	TableName        string
	PingTableName    string
	Separator        string
	JoinedIdentifier JoinedIdentifier
	SystemTracking   bool
}

// NewSQLiteDriver creates a new SQLite driver.
func NewSQLiteDriver(db *sql.DB, tableName string, joinedIdentifier JoinedIdentifier) *SQLiteDriver {
	if tableName == "" {
		tableName = "trifle_stats"
	}
	return &SQLiteDriver{
		DB:               db,
		TableName:        tableName,
		PingTableName:    tableName + "_ping",
		Separator:        "::",
		JoinedIdentifier: joinedIdentifier,
		SystemTracking:   true,
	}
}

// Setup initializes the table schema for the configured identifier mode.
func (d *SQLiteDriver) Setup(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.DB == nil {
		return fmt.Errorf("sqlite driver requires DB")
	}
	if err := d.applyPragmas(ctx); err != nil {
		return err
	}

	var query string
	switch d.JoinedIdentifier {
	case JoinedPartial:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT NOT NULL, at TEXT NOT NULL, data TEXT NOT NULL DEFAULT '{}', PRIMARY KEY (key, at));`, d.TableName)
	case JoinedSeparated:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT NOT NULL, granularity TEXT NOT NULL, at TEXT NOT NULL, data TEXT NOT NULL DEFAULT '{}', PRIMARY KEY (key, granularity, at));`, d.TableName)
	default:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, data TEXT NOT NULL DEFAULT '{}');`, d.TableName)
	}
	if _, err := d.DB.ExecContext(ctx, query); err != nil {
		return err
	}

	if d.JoinedIdentifier == JoinedSeparated {
		pingQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, at TEXT NOT NULL, data TEXT NOT NULL DEFAULT '{}');`, d.pingTable())
		if _, err := d.DB.ExecContext(ctx, pingQuery); err != nil {
			return err
		}
	}
	return nil
}

func (d *SQLiteDriver) pingTable() string {
	if d.PingTableName != "" {
		return d.PingTableName
	}
	return d.TableName + "_ping"
}

func (d *SQLiteDriver) Description() string {
	mode := "J"
	if d.JoinedIdentifier == JoinedPartial {
		mode = "P"
	} else if d.JoinedIdentifier == JoinedSeparated {
		mode = "S"
	}
	return fmt.Sprintf("SQLiteDriver(%s)", mode)
}

// Inc increments numeric values in-place.
func (d *SQLiteDriver) Inc(ctx context.Context, keys []Key, values map[string]any) error {
	return d.IncCount(ctx, keys, values, 1)
}

// IncCount increments values and records system tracking count.
func (d *SQLiteDriver) IncCount(ctx context.Context, keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(ctx, keys, values, "inc", count)
}

// Set sets provided values (without deleting other keys).
func (d *SQLiteDriver) Set(ctx context.Context, keys []Key, values map[string]any) error {
	return d.SetCount(ctx, keys, values, 1)
}

// SetCount sets values and records system tracking count.
func (d *SQLiteDriver) SetCount(ctx context.Context, keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(ctx, keys, values, "set", count)
}

// Get fetches values for keys in order.
func (d *SQLiteDriver) Get(ctx context.Context, keys []Key) ([]map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return []map[string]any{}, nil
	}

	identifiers := make([]identifier, 0, len(keys))
	for _, k := range keys {
		ident, err := d.identifierForKey(k)
		if err != nil {
			return nil, err
		}
		identifiers = append(identifiers, ident)
	}

	query, args := buildGetQuery(d.TableName, identifiers)
	rows, err := d.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resultMap := map[string]map[string]any{}
	for rows.Next() {
		rowIdent, dataJSON, err := scanRow(d.JoinedIdentifier, rows)
		if err != nil {
			return nil, err
		}
		var packed map[string]any
		if dataJSON == "" {
			packed = map[string]any{}
		} else {
			if err := json.Unmarshal([]byte(dataJSON), &packed); err != nil {
				packed = map[string]any{}
			}
		}
		resultMap[rowIdent] = packed
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]map[string]any, 0, len(keys))
	for _, ident := range identifiers {
		packed := resultMap[ident.lookupKey]
		results = append(results, Unpack(packed))
	}
	return results, nil
}

// Ping stores the latest status payload for the key (separated mode only).
func (d *SQLiteDriver) Ping(ctx context.Context, key Key, values map[string]any) error {
	if d.JoinedIdentifier != JoinedSeparated {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if d.DB == nil {
		return fmt.Errorf("sqlite driver requires DB")
	}
	if key.At == nil {
		return fmt.Errorf("ping requires At")
	}

	packed := Pack(map[string]any{"data": values, "at": key.At.Unix()})
	dataJSON, err := json.Marshal(packed)
	if err != nil {
		return err
	}
	atFormatted := formatAt(*key.At)

	query := fmt.Sprintf(
		`INSERT INTO %s (key, at, data) VALUES (?, ?, json(?)) ON CONFLICT (key) DO UPDATE SET at = ?, data = json(?);`,
		d.pingTable(),
	)

	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err := tx.ExecContext(ctx, query, key.Key, atFormatted, string(dataJSON), atFormatted, string(dataJSON)); err != nil {
		return err
	}
	return tx.Commit()
}

// Scan returns the latest status for the key (separated mode only).
func (d *SQLiteDriver) Scan(ctx context.Context, key Key) (time.Time, map[string]any, bool, error) {
	if d.JoinedIdentifier != JoinedSeparated {
		return time.Time{}, nil, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if d.DB == nil {
		return time.Time{}, nil, false, fmt.Errorf("sqlite driver requires DB")
	}

	query := fmt.Sprintf(`SELECT at, data FROM %s WHERE key = ? ORDER BY at DESC LIMIT 1;`, d.pingTable())
	var atRaw, dataRaw string
	err := d.DB.QueryRowContext(ctx, query, key.Key).Scan(&atRaw, &dataRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil, false, nil
	}
	if err != nil {
		return time.Time{}, nil, false, err
	}

	at, err := time.Parse(time.RFC3339, atRaw)
	if err != nil {
		return time.Time{}, nil, false, err
	}
	var packed map[string]any
	if err := json.Unmarshal([]byte(dataRaw), &packed); err != nil {
		return time.Time{}, nil, false, nil
	}
	return at.UTC(), Unpack(packed), true, nil
}

// --- internal helpers ---

type identifier struct {
	columns   []string
	values    []any
	lookupKey string
}

func (d *SQLiteDriver) identifierForKey(k Key) (identifier, error) {
	switch d.JoinedIdentifier {
	case JoinedPartial:
		if k.At == nil {
			return identifier{}, fmt.Errorf("partial identifier requires At")
		}
		val := k.PartialJoin(d.Separator)
		at := formatAt(*k.At)
		return identifier{
			columns:   []string{"key", "at"},
			values:    []any{val, at},
			lookupKey: val + "|" + at,
		}, nil
	case JoinedSeparated:
		if k.At == nil {
			return identifier{}, fmt.Errorf("separated identifier requires At")
		}
		at := formatAt(*k.At)
		return identifier{
			columns:   []string{"key", "granularity", "at"},
			values:    []any{k.Key, k.Granularity, at},
			lookupKey: k.Key + "|" + k.Granularity + "|" + at,
		}, nil
	default:
		val := k.Join(d.Separator)
		return identifier{
			columns:   []string{"key"},
			values:    []any{val},
			lookupKey: val,
		}, nil
	}
}

func (d *SQLiteDriver) writeWithOperation(ctx context.Context, keys []Key, values map[string]any, op string, count int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return nil
	}
	if d.DB == nil {
		return fmt.Errorf("sqlite driver requires DB")
	}

	packed := Pack(values)
	if len(packed) == 0 {
		return nil
	}

	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, k := range keys {
		ident, err := d.identifierForKey(k)
		if err != nil {
			return err
		}
		if err := d.batchWrite(ctx, tx, ident, packed, op); err != nil {
			return err
		}
		if d.SystemTracking {
			systemKey := Key{
				Key:         systemKeyName,
				Granularity: k.Granularity,
				At:          k.At,
			}
			systemIdent, err := d.identifierForKey(systemKey)
			if err != nil {
				return err
			}
			systemData := systemDataFor(k.SystemTrackingKey(), count)
			if err := d.batchWrite(ctx, tx, systemIdent, systemData, "inc"); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (d *SQLiteDriver) batchWrite(ctx context.Context, tx *sql.Tx, ident identifier, packed map[string]any, op string) error {
	const batchSize = 10

	keys := make([]string, 0, len(packed))
	for k := range packed {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batchKeys := keys[i:end]
		batch := make(map[string]any, len(batchKeys))
		for _, k := range batchKeys {
			batch[k] = packed[k]
		}
		query, args, err := buildWriteQuery(d.TableName, ident, batch, op)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

const systemKeyName = "__system__key__"

func systemDataFor(key string, count int64) map[string]any {
	return Pack(map[string]any{
		"count": count,
		"keys": map[string]any{
			key: count,
		},
	})
}

func buildWriteQuery(table string, ident identifier, batch map[string]any, op string) (string, []any, error) {
	columns := ident.columns
	colList := strings.Join(columns, ", ")
	placeholders := make([]string, 0, len(columns)+1)
	args := make([]any, 0, len(columns)+1+len(batch))

	for _, v := range ident.values {
		placeholders = append(placeholders, "?")
		args = append(args, v)
	}

	batchJSON, err := json.Marshal(batch)
	if err != nil {
		return "", nil, err
	}
	placeholders = append(placeholders, "json(?)")
	args = append(args, string(batchJSON))

	conflict := strings.Join(columns, ", ")
	jsonExpr, valArgs := buildJSONExpression(batch, op)
	args = append(args, valArgs...)

	query := fmt.Sprintf(
		"INSERT INTO %s (%s, data) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET data = %s;",
		table,
		colList,
		strings.Join(placeholders, ", "),
		conflict,
		jsonExpr,
	)
	return query, args, nil
}

func buildJSONExpression(batch map[string]any, op string) (string, []any) {
	keys := make([]string, 0, len(batch))
	for k := range batch {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	expr := "data"
	args := []any{}

	for _, k := range keys {
		path := jsonPathForKey(k)
		switch op {
		case "inc":
			expr = fmt.Sprintf(
				"json_set(%s, '%s', IFNULL(json_extract(data, '%s'), 0) + ?)",
				expr, path, path,
			)
			args = append(args, batch[k])
		case "set":
			expr = fmt.Sprintf(
				"json_set(%s, '%s', ?)",
				expr, path,
			)
			args = append(args, batch[k])
		default:
			panic("invalid op")
		}
	}
	return expr, args
}

func buildGetQuery(table string, identifiers []identifier) (string, []any) {
	conds := make([]string, 0, len(identifiers))
	args := []any{}

	for _, ident := range identifiers {
		if len(ident.columns) == 0 {
			continue
		}
		parts := make([]string, 0, len(ident.columns))
		for idx, col := range ident.columns {
			if col == "at" {
				at := fmt.Sprint(ident.values[idx])
				parts = append(parts, "(at = ? OR at = ?)")
				args = append(args, at, withZeroMicrosRFC3339(at))
				continue
			}
			parts = append(parts, fmt.Sprintf("%s = ?", col))
			args = append(args, ident.values[idx])
		}
		conds = append(conds, "("+strings.Join(parts, " AND ")+")")
	}

	if len(conds) == 0 {
		return fmt.Sprintf("SELECT * FROM %s WHERE 1 = 0;", table), []any{}
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s;", table, strings.Join(conds, " OR ")), args
}

func scanRow(mode JoinedIdentifier, rows *sql.Rows) (string, string, error) {
	switch mode {
	case JoinedPartial:
		var key, at, data string
		if err := rows.Scan(&key, &at, &data); err != nil {
			return "", "", err
		}
		return key + "|" + normalizeSQLiteAtForLookup(at), data, nil
	case JoinedSeparated:
		var key, granularity, at, data string
		if err := rows.Scan(&key, &granularity, &at, &data); err != nil {
			return "", "", err
		}
		return key + "|" + granularity + "|" + normalizeSQLiteAtForLookup(at), data, nil
	default:
		var key string
		var data string
		if err := rows.Scan(&key, &data); err != nil {
			return "", "", err
		}
		return key, data, nil
	}
}

func formatAt(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func withZeroMicrosRFC3339(value string) string {
	if strings.HasSuffix(value, "Z") && !strings.Contains(value, ".") {
		return strings.TrimSuffix(value, "Z") + ".000000Z"
	}
	return value
}

func normalizeSQLiteAtForLookup(value string) string {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, value)
		if err != nil {
			return value
		}
	}
	return formatAt(parsed)
}

func jsonPathForKey(key string) string {
	escaped := strings.ReplaceAll(key, "'", "''")
	return fmt.Sprintf("$.%s", escaped)
}

func (d *SQLiteDriver) applyPragmas(ctx context.Context) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, p := range pragmas {
		if _, err := d.DB.ExecContext(ctx, p); err != nil {
			return err
		}
	}
	return nil
}
