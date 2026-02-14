package triflestats

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLDriver implements the Driver interface using MySQL JSON storage.
type MySQLDriver struct {
	DB               *sql.DB
	TableName        string
	Separator        string
	JoinedIdentifier JoinedIdentifier
	SystemTracking   bool
}

// NewMySQLDriver creates a new MySQL driver.
func NewMySQLDriver(db *sql.DB, tableName string, joinedIdentifier JoinedIdentifier) *MySQLDriver {
	if tableName == "" {
		tableName = "trifle_stats"
	}
	return &MySQLDriver{
		DB:               db,
		TableName:        tableName,
		Separator:        "::",
		JoinedIdentifier: joinedIdentifier,
		SystemTracking:   true,
	}
}

// Setup initializes table schema for the configured identifier mode.
func (d *MySQLDriver) Setup() error {
	if d.DB == nil {
		return fmt.Errorf("mysql driver requires DB")
	}

	var query string
	table := quoteMySQLIdentifier(d.TableName)
	switch d.JoinedIdentifier {
	case JoinedFull:
		query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`key` VARCHAR(255) PRIMARY KEY, `data` JSON NOT NULL);", table)
	case JoinedPartial:
		query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`key` VARCHAR(255) NOT NULL, `at` DATETIME(6) NOT NULL, `data` JSON NOT NULL, PRIMARY KEY (`key`, `at`));", table)
	case JoinedSeparated:
		query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`key` VARCHAR(255) NOT NULL, `granularity` VARCHAR(255) NOT NULL, `at` DATETIME(6) NOT NULL, `data` JSON NOT NULL, PRIMARY KEY (`key`, `granularity`, `at`));", table)
	default:
		query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (`key` VARCHAR(255) PRIMARY KEY, `data` JSON NOT NULL);", table)
	}

	_, err := d.DB.Exec(query)
	return err
}

func (d *MySQLDriver) Description() string {
	mode := "J"
	if d.JoinedIdentifier == JoinedPartial {
		mode = "P"
	} else if d.JoinedIdentifier == JoinedSeparated {
		mode = "S"
	}
	return fmt.Sprintf("MySQLDriver(%s)", mode)
}

// Inc increments numeric values in-place.
func (d *MySQLDriver) Inc(keys []Key, values map[string]any) error {
	return d.IncCount(keys, values, 1)
}

// IncCount increments values and records system tracking count.
func (d *MySQLDriver) IncCount(keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(keys, values, "inc", count)
}

// Set sets provided values without deleting unspecified keys.
func (d *MySQLDriver) Set(keys []Key, values map[string]any) error {
	return d.SetCount(keys, values, 1)
}

// SetCount sets values and records system tracking count.
func (d *MySQLDriver) SetCount(keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(keys, values, "set", count)
}

// Get fetches values for keys in order.
func (d *MySQLDriver) Get(keys []Key) ([]map[string]any, error) {
	if len(keys) == 0 {
		return []map[string]any{}, nil
	}
	if d.DB == nil {
		return nil, fmt.Errorf("mysql driver requires DB")
	}

	identifiers := make([]identifier, 0, len(keys))
	for _, key := range keys {
		ident, err := d.identifierForKey(key)
		if err != nil {
			return nil, err
		}
		identifiers = append(identifiers, ident)
	}

	query, args := buildMySQLGetQuery(d.TableName, d.JoinedIdentifier, identifiers)
	rows, err := d.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resultMap := map[string]map[string]any{}
	for rows.Next() {
		lookup, dataJSON, err := scanMySQLRow(d.JoinedIdentifier, rows)
		if err != nil {
			return nil, err
		}

		var packed map[string]any
		if dataJSON == "" {
			packed = map[string]any{}
		} else if err := json.Unmarshal([]byte(dataJSON), &packed); err != nil {
			packed = map[string]any{}
		}
		resultMap[lookup] = packed
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		ident, err := d.identifierForKey(key)
		if err != nil {
			return nil, err
		}
		results = append(results, Unpack(resultMap[ident.lookupKey]))
	}
	return results, nil
}

func (d *MySQLDriver) writeWithOperation(keys []Key, values map[string]any, op string, count int64) error {
	if len(keys) == 0 {
		return nil
	}
	if d.DB == nil {
		return fmt.Errorf("mysql driver requires DB")
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

	for _, key := range keys {
		ident, err := d.identifierForKey(key)
		if err != nil {
			return err
		}
		query, args, err := buildMySQLWriteQuery(d.TableName, ident, packed, op)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(query, args...); err != nil {
			return err
		}

		if d.SystemTracking {
			systemKey := Key{
				Key:         systemKeyName,
				Granularity: key.Granularity,
				At:          key.At,
				TrackingKey: key.TrackingKey,
			}
			systemIdent, err := d.identifierForKey(systemKey)
			if err != nil {
				return err
			}
			systemQuery, systemArgs, err := buildMySQLWriteQuery(
				d.TableName,
				systemIdent,
				systemDataFor(key.SystemTrackingKey(), count),
				"inc",
			)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(systemQuery, systemArgs...); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (d *MySQLDriver) identifierForKey(k Key) (identifier, error) {
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
		at := formatMySQLAt(*k.At)
		return identifier{
			columns:   []string{"key", "at"},
			values:    []any{val, *k.At},
			lookupKey: val + "|" + at,
		}, nil
	case JoinedSeparated:
		if k.At == nil {
			return identifier{}, fmt.Errorf("separated identifier requires At")
		}
		at := formatMySQLAt(*k.At)
		return identifier{
			columns:   []string{"key", "granularity", "at"},
			values:    []any{k.Key, k.Granularity, *k.At},
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

func buildMySQLWriteQuery(table string, ident identifier, packed map[string]any, op string) (string, []any, error) {
	columns := ident.columns
	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, quoteMySQLIdentifier(column))
	}
	colList := strings.Join(quotedColumns, ", ")
	placeholders := make([]string, 0, len(columns)+1)
	args := make([]any, 0, len(columns)+1+len(packed))

	for _, v := range ident.values {
		placeholders = append(placeholders, "?")
		args = append(args, normalizeMySQLQueryValue(v))
	}

	batchJSON, err := json.Marshal(packed)
	if err != nil {
		return "", nil, err
	}
	placeholders = append(placeholders, "CAST(? AS JSON)")
	args = append(args, string(batchJSON))

	jsonExpr, exprArgs, err := buildMySQLJSONExpression(packed, op)
	if err != nil {
		return "", nil, err
	}
	args = append(args, exprArgs...)

	query := fmt.Sprintf(
		"INSERT INTO %s (%s, `data`) VALUES (%s) ON DUPLICATE KEY UPDATE `data` = %s;",
		quoteMySQLIdentifier(table),
		colList,
		strings.Join(placeholders, ", "),
		jsonExpr,
	)
	return query, args, nil
}

func buildMySQLJSONExpression(packed map[string]any, op string) (string, []any, error) {
	keys := make([]string, 0, len(packed))
	for key := range packed {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	expr := "JSON_SET(COALESCE(`data`, JSON_OBJECT())"
	args := []any{}

	for _, key := range keys {
		path := mysqlJSONPathForKey(key)
		switch op {
		case "inc":
			delta, ok := toFloat(packed[key])
			if !ok {
				return "", nil, fmt.Errorf("increment requires numeric value for key %q", key)
			}
			expr += fmt.Sprintf(
				", '%s', (COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(COALESCE(`data`, JSON_OBJECT()), '%s')) AS DECIMAL(65,10)), 0) + CAST(? AS DECIMAL(65,10)))",
				path,
				path,
			)
			args = append(args, delta)
		case "set":
			encoded, err := json.Marshal(packed[key])
			if err != nil {
				return "", nil, err
			}
			expr += fmt.Sprintf(", '%s', CAST(? AS JSON)", path)
			args = append(args, string(encoded))
		default:
			return "", nil, fmt.Errorf("invalid operation: %s", op)
		}
	}

	expr += ")"
	return expr, args, nil
}

func buildMySQLGetQuery(table string, mode JoinedIdentifier, identifiers []identifier) (string, []any) {
	conds := make([]string, 0, len(identifiers))
	args := []any{}

	for _, ident := range identifiers {
		parts := make([]string, 0, len(ident.columns))
		for _, col := range ident.columns {
			parts = append(parts, fmt.Sprintf("%s = ?", quoteMySQLIdentifier(col)))
		}
		conds = append(conds, "("+strings.Join(parts, " AND ")+")")
		for _, value := range ident.values {
			args = append(args, normalizeMySQLQueryValue(value))
		}
	}

	selectColumns := "`key`, CAST(`data` AS CHAR) AS data"
	if mode == JoinedPartial {
		selectColumns = "`key`, `at`, CAST(`data` AS CHAR) AS data"
	} else if mode == JoinedSeparated {
		selectColumns = "`key`, `granularity`, `at`, CAST(`data` AS CHAR) AS data"
	}

	if len(conds) == 0 {
		return fmt.Sprintf("SELECT %s FROM %s WHERE 1 = 0;", selectColumns, quoteMySQLIdentifier(table)), []any{}
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s;", selectColumns, quoteMySQLIdentifier(table), strings.Join(conds, " OR ")), args
}

func scanMySQLRow(mode JoinedIdentifier, rows *sql.Rows) (string, string, error) {
	switch mode {
	case JoinedFull:
		var key string
		var data string
		if err := rows.Scan(&key, &data); err != nil {
			return "", "", err
		}
		return key, data, nil
	case JoinedPartial:
		var key string
		var atRaw any
		var data string
		if err := rows.Scan(&key, &atRaw, &data); err != nil {
			return "", "", err
		}
		at, err := parseMySQLAtValue(atRaw)
		if err != nil {
			return "", "", err
		}
		return key + "|" + formatMySQLAt(at), data, nil
	case JoinedSeparated:
		var key, granularity string
		var atRaw any
		var data string
		if err := rows.Scan(&key, &granularity, &atRaw, &data); err != nil {
			return "", "", err
		}
		at, err := parseMySQLAtValue(atRaw)
		if err != nil {
			return "", "", err
		}
		return key + "|" + granularity + "|" + formatMySQLAt(at), data, nil
	default:
		var key string
		var data string
		if err := rows.Scan(&key, &data); err != nil {
			return "", "", err
		}
		return key, data, nil
	}
}

func normalizeMySQLQueryValue(value any) any {
	switch node := value.(type) {
	case time.Time:
		return formatMySQLAt(node)
	default:
		return value
	}
}

func mysqlJSONPathForKey(key string) string {
	escaped := strings.ReplaceAll(key, "\\", "\\\\")
	escaped = strings.ReplaceAll(escaped, "\"", "\\\"")
	escaped = strings.ReplaceAll(escaped, "'", "''")
	return fmt.Sprintf("$.\"%s\"", escaped)
}

func quoteMySQLIdentifier(identifier string) string {
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return "`" + escaped + "`"
}

func parseMySQLAtValue(value any) (time.Time, error) {
	switch node := value.(type) {
	case time.Time:
		return node.UTC(), nil
	case []byte:
		return parseMySQLAtString(string(node))
	case string:
		return parseMySQLAtString(node)
	default:
		return time.Time{}, fmt.Errorf("unsupported mysql at value type %T", value)
	}
}

func parseMySQLAtString(value string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
	}
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, time.UTC)
		if err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to parse mysql datetime %q", value)
}

func formatMySQLAt(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000000")
}
