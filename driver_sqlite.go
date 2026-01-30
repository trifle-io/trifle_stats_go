package triflestats

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteDriver implements the Driver interface using SQLite JSON1.
type SQLiteDriver struct {
	DB              *sql.DB
	TableName       string
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
		DB:              db,
		TableName:       tableName,
		Separator:        "::",
		JoinedIdentifier: joinedIdentifier,
		SystemTracking:   true,
	}
}

// Setup initializes the table schema for the configured identifier mode.
func (d *SQLiteDriver) Setup() error {
	if d.DB == nil {
		return fmt.Errorf("sqlite driver requires DB")
	}
	if err := d.applyPragmas(); err != nil {
		return err
	}

	var query string
	switch d.JoinedIdentifier {
	case JoinedFull:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, data TEXT NOT NULL DEFAULT '{}');`, d.TableName)
	case JoinedPartial:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT NOT NULL, at TEXT NOT NULL, data TEXT NOT NULL DEFAULT '{}', PRIMARY KEY (key, at));`, d.TableName)
	case JoinedSeparated:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT NOT NULL, granularity TEXT NOT NULL, at TEXT NOT NULL, data TEXT NOT NULL DEFAULT '{}', PRIMARY KEY (key, granularity, at));`, d.TableName)
	default:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, data TEXT NOT NULL DEFAULT '{}');`, d.TableName)
	}
	_, err := d.DB.Exec(query)
	return err
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
func (d *SQLiteDriver) Inc(keys []Key, values map[string]any) error {
	return d.writeWithOperation(keys, values, "inc")
}

// Set sets provided values (without deleting other keys).
func (d *SQLiteDriver) Set(keys []Key, values map[string]any) error {
	return d.writeWithOperation(keys, values, "set")
}

// Get fetches values for keys in order.
func (d *SQLiteDriver) Get(keys []Key) ([]map[string]any, error) {
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
	rows, err := d.DB.Query(query, args...)
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
	for _, k := range keys {
		ident, err := d.identifierForKey(k)
		if err != nil {
			return nil, err
		}
		packed := resultMap[ident.lookupKey]
		results = append(results, Unpack(packed))
	}
	return results, nil
}

// --- internal helpers ---

type identifier struct {
	columns   []string
	values    []any
	lookupKey string
}

func (d *SQLiteDriver) identifierForKey(k Key) (identifier, error) {
	switch d.JoinedIdentifier {
	case JoinedFull:
		val := k.Join(d.Separator)
		return identifier{
			columns:   []string{"key"},
			values:    []any{val},
			lookupKey: val,
		}, nil
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

func (d *SQLiteDriver) writeWithOperation(keys []Key, values map[string]any, op string) error {
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

	tx, err := d.DB.Begin()
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
		if err := d.batchWrite(tx, ident, packed, op); err != nil {
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
			systemData := systemDataFor(k.Key, 1)
			if err := d.batchWrite(tx, systemIdent, systemData, "inc"); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (d *SQLiteDriver) batchWrite(tx *sql.Tx, ident identifier, packed map[string]any, op string) error {
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
		if _, err := tx.Exec(query, args...); err != nil {
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
		for _, col := range ident.columns {
			parts = append(parts, fmt.Sprintf("%s = ?", col))
		}
		conds = append(conds, "("+strings.Join(parts, " AND ")+")")
		args = append(args, ident.values...)
	}

	if len(conds) == 0 {
		return fmt.Sprintf("SELECT * FROM %s WHERE 1 = 0;", table), []any{}
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s;", table, strings.Join(conds, " OR ")), args
}

func scanRow(mode JoinedIdentifier, rows *sql.Rows) (string, string, error) {
	switch mode {
	case JoinedFull:
		var key string
		var data string
		if err := rows.Scan(&key, &data); err != nil {
			return "", "", err
		}
		return key, data, nil
	case JoinedPartial:
		var key, at, data string
		if err := rows.Scan(&key, &at, &data); err != nil {
			return "", "", err
		}
		return key + "|" + at, data, nil
	case JoinedSeparated:
		var key, granularity, at, data string
		if err := rows.Scan(&key, &granularity, &at, &data); err != nil {
			return "", "", err
		}
		return key + "|" + granularity + "|" + at, data, nil
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
	return t.Format("2006-01-02 15:04:05")
}

func jsonPathForKey(key string) string {
	escaped := strings.ReplaceAll(key, "'", "''")
	return fmt.Sprintf("$.%s", escaped)
}

func (d *SQLiteDriver) applyPragmas() error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, p := range pragmas {
		if _, err := d.DB.Exec(p); err != nil {
			return err
		}
	}
	return nil
}
