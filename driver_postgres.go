package triflestats

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresDriver implements the Driver interface using PostgreSQL JSONB storage.
type PostgresDriver struct {
	DB               *sql.DB
	TableName        string
	PingTableName    string
	Separator        string
	JoinedIdentifier JoinedIdentifier
	SystemTracking   bool
}

// NewPostgresDriver creates a PostgreSQL driver.
func NewPostgresDriver(db *sql.DB, tableName string, joinedIdentifier JoinedIdentifier) *PostgresDriver {
	if tableName == "" {
		tableName = "trifle_stats"
	}
	return &PostgresDriver{
		DB:               db,
		TableName:        tableName,
		PingTableName:    tableName + "_ping",
		Separator:        "::",
		JoinedIdentifier: joinedIdentifier,
		SystemTracking:   true,
	}
}

// Setup initializes table schema for the configured identifier mode.
func (d *PostgresDriver) Setup(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.DB == nil {
		return fmt.Errorf("postgres driver requires DB")
	}

	var query string
	switch d.JoinedIdentifier {
	case JoinedPartial:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) NOT NULL, at TIMESTAMPTZ NOT NULL, data JSONB NOT NULL DEFAULT '{}'::jsonb, PRIMARY KEY (key, at));`, d.TableName)
	case JoinedSeparated:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) NOT NULL, granularity VARCHAR(255) NOT NULL, at TIMESTAMPTZ NOT NULL, data JSONB NOT NULL DEFAULT '{}'::jsonb, PRIMARY KEY (key, granularity, at));`, d.TableName)
	default:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'::jsonb);`, d.TableName)
	}

	if _, err := d.DB.ExecContext(ctx, query); err != nil {
		return err
	}

	if d.JoinedIdentifier == JoinedSeparated {
		pingQuery := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) PRIMARY KEY, at TIMESTAMPTZ NOT NULL, data JSONB NOT NULL DEFAULT '{}'::jsonb);`, d.pingTable())
		if _, err := d.DB.ExecContext(ctx, pingQuery); err != nil {
			return err
		}
	}
	return nil
}

func (d *PostgresDriver) pingTable() string {
	if d.PingTableName != "" {
		return d.PingTableName
	}
	return d.TableName + "_ping"
}

func (d *PostgresDriver) Description() string {
	mode := "J"
	if d.JoinedIdentifier == JoinedPartial {
		mode = "P"
	} else if d.JoinedIdentifier == JoinedSeparated {
		mode = "S"
	}
	return fmt.Sprintf("PostgresDriver(%s)", mode)
}

// Inc increments numeric values in-place.
func (d *PostgresDriver) Inc(ctx context.Context, keys []Key, values map[string]any) error {
	return d.IncCount(ctx, keys, values, 1)
}

// IncCount increments values and records system tracking count.
func (d *PostgresDriver) IncCount(ctx context.Context, keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(ctx, keys, values, "inc", count)
}

// Set sets provided values without deleting other keys.
func (d *PostgresDriver) Set(ctx context.Context, keys []Key, values map[string]any) error {
	return d.SetCount(ctx, keys, values, 1)
}

// SetCount sets provided values and records system tracking count.
func (d *PostgresDriver) SetCount(ctx context.Context, keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(ctx, keys, values, "set", count)
}

// Get fetches values in key order using a single SELECT across all identifiers.
func (d *PostgresDriver) Get(ctx context.Context, keys []Key) ([]map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return []map[string]any{}, nil
	}
	if d.DB == nil {
		return nil, fmt.Errorf("postgres driver requires DB")
	}

	identifiers := make([]identifier, 0, len(keys))
	for _, key := range keys {
		ident, err := d.identifierForKey(key)
		if err != nil {
			return nil, err
		}
		identifiers = append(identifiers, ident)
	}

	query, args := d.buildGetQuery(identifiers)
	rows, err := d.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resultMap := map[string]map[string]any{}
	for rows.Next() {
		lookup, dataJSON, err := d.scanGetRow(rows)
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
	for _, ident := range identifiers {
		results = append(results, Unpack(resultMap[ident.lookupKey]))
	}
	return results, nil
}

// Ping stores the latest status payload for the key (separated mode only).
func (d *PostgresDriver) Ping(ctx context.Context, key Key, values map[string]any) error {
	if d.JoinedIdentifier != JoinedSeparated {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if d.DB == nil {
		return fmt.Errorf("postgres driver requires DB")
	}
	if key.At == nil {
		return fmt.Errorf("ping requires At")
	}

	packed := Pack(map[string]any{"data": values, "at": key.At.Unix()})
	dataJSON, err := json.Marshal(packed)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (key, at, data) VALUES ($1, $2, $3::jsonb) ON CONFLICT (key) DO UPDATE SET at = $2, data = $3::jsonb;`,
		d.pingTable(),
	)

	tx, err := d.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err := tx.ExecContext(ctx, query, key.Key, key.At.UTC(), string(dataJSON)); err != nil {
		return err
	}
	return tx.Commit()
}

// Scan returns the latest status for the key (separated mode only).
func (d *PostgresDriver) Scan(ctx context.Context, key Key) (time.Time, map[string]any, bool, error) {
	if d.JoinedIdentifier != JoinedSeparated {
		return time.Time{}, nil, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if d.DB == nil {
		return time.Time{}, nil, false, fmt.Errorf("postgres driver requires DB")
	}

	query := fmt.Sprintf(`SELECT at, data FROM %s WHERE key = $1 ORDER BY at DESC LIMIT 1;`, d.pingTable())
	var at time.Time
	var raw []byte
	err := d.DB.QueryRowContext(ctx, query, key.Key).Scan(&at, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil, false, nil
	}
	if err != nil {
		return time.Time{}, nil, false, err
	}

	var packed map[string]any
	if err := json.Unmarshal(raw, &packed); err != nil {
		return time.Time{}, nil, false, nil
	}
	return at.UTC(), Unpack(packed), true, nil
}

func (d *PostgresDriver) writeWithOperation(ctx context.Context, keys []Key, values map[string]any, op string, count int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return nil
	}
	if d.DB == nil {
		return fmt.Errorf("postgres driver requires DB")
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

	for _, key := range keys {
		ident, err := d.identifierForKey(key)
		if err != nil {
			return err
		}
		query, args, err := d.buildUpsertQuery(ident, packed, op)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}

		if d.SystemTracking {
			systemKey := Key{
				Key:         systemKeyName,
				Granularity: key.Granularity,
				At:          key.At,
			}
			systemIdent, err := d.identifierForKey(systemKey)
			if err != nil {
				return err
			}
			systemQuery, systemArgs, err := d.buildUpsertQuery(systemIdent, systemDataFor(key.SystemTrackingKey(), count), "inc")
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, systemQuery, systemArgs...); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// buildUpsertQuery builds an atomic INSERT ... ON CONFLICT DO UPDATE that
// mirrors the Ruby driver's inc_query/set_query jsonb_set chains.
func (d *PostgresDriver) buildUpsertQuery(ident identifier, packed map[string]any, op string) (string, []any, error) {
	columns := strings.Join(ident.columns, ", ")
	placeholders := make([]string, 0, len(ident.columns))
	args := make([]any, 0, len(ident.values)+1+len(packed))
	for idx, value := range ident.values {
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx+1))
		args = append(args, value)
	}

	dataJSON, err := json.Marshal(packed)
	if err != nil {
		return "", nil, err
	}
	args = append(args, string(dataJSON))

	fields := make([]string, 0, len(packed))
	for field := range packed {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	expression := fmt.Sprintf("to_jsonb(%s.data)", d.TableName)
	switch op {
	case "inc":
		for _, field := range fields {
			delta, ok := toFloat(packed[field])
			if !ok {
				return "", nil, fmt.Errorf("increment requires numeric value for key %q", field)
			}
			path := escapePostgresString(field)
			expression = fmt.Sprintf(
				"jsonb_set(%s, ARRAY['%s'], (COALESCE(%s.data->>'%s', '0')::numeric + %s)::text::jsonb)",
				expression, path, d.TableName, path, strconv.FormatFloat(delta, 'f', -1, 64),
			)
		}
	case "set":
		for _, field := range fields {
			encoded, err := json.Marshal(packed[field])
			if err != nil {
				return "", nil, err
			}
			args = append(args, string(encoded))
			expression = fmt.Sprintf(
				"jsonb_set(%s, ARRAY['%s'], $%d::jsonb)",
				expression, escapePostgresString(field), len(args),
			)
		}
	default:
		return "", nil, fmt.Errorf("invalid operation: %s", op)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s, data) VALUES (%s, $%d) ON CONFLICT (%s) DO UPDATE SET data = %s;`,
		d.TableName,
		columns,
		strings.Join(placeholders, ", "),
		len(ident.values)+1,
		columns,
		expression,
	)
	return query, args, nil
}

func (d *PostgresDriver) buildGetQuery(identifiers []identifier) (string, []any) {
	selectColumns := "key, data"
	if d.JoinedIdentifier == JoinedPartial {
		selectColumns = "key, at, data"
	} else if d.JoinedIdentifier == JoinedSeparated {
		selectColumns = "key, granularity, at, data"
	}

	conds := make([]string, 0, len(identifiers))
	args := []any{}
	for _, ident := range identifiers {
		parts := make([]string, 0, len(ident.columns))
		for idx, col := range ident.columns {
			args = append(args, ident.values[idx])
			parts = append(parts, fmt.Sprintf("%s = $%d", col, len(args)))
		}
		conds = append(conds, "("+strings.Join(parts, " AND ")+")")
	}

	if len(conds) == 0 {
		return fmt.Sprintf("SELECT %s FROM %s WHERE 1 = 0;", selectColumns, d.TableName), []any{}
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s;", selectColumns, d.TableName, strings.Join(conds, " OR ")), args
}

func (d *PostgresDriver) scanGetRow(rows *sql.Rows) (string, string, error) {
	switch d.JoinedIdentifier {
	case JoinedPartial:
		var key string
		var at time.Time
		var data string
		if err := rows.Scan(&key, &at, &data); err != nil {
			return "", "", err
		}
		return key + "|" + postgresAtLookup(at), data, nil
	case JoinedSeparated:
		var key, granularity string
		var at time.Time
		var data string
		if err := rows.Scan(&key, &granularity, &at, &data); err != nil {
			return "", "", err
		}
		return key + "|" + granularity + "|" + postgresAtLookup(at), data, nil
	default:
		var key string
		var data string
		if err := rows.Scan(&key, &data); err != nil {
			return "", "", err
		}
		return key, data, nil
	}
}

func (d *PostgresDriver) identifierForKey(k Key) (identifier, error) {
	switch d.JoinedIdentifier {
	case JoinedPartial:
		if k.At == nil {
			return identifier{}, fmt.Errorf("partial identifier requires At")
		}
		val := k.PartialJoin(d.Separator)
		return identifier{
			columns:   []string{"key", "at"},
			values:    []any{val, k.At.UTC()},
			lookupKey: val + "|" + postgresAtLookup(*k.At),
		}, nil
	case JoinedSeparated:
		if k.At == nil {
			return identifier{}, fmt.Errorf("separated identifier requires At")
		}
		return identifier{
			columns:   []string{"key", "granularity", "at"},
			values:    []any{k.Key, k.Granularity, k.At.UTC()},
			lookupKey: k.Key + "|" + k.Granularity + "|" + postgresAtLookup(*k.At),
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

// postgresAtLookup normalizes a timestamp to UTC unix seconds so that stored
// TIMESTAMPTZ values and request keys always agree regardless of time zone.
func postgresAtLookup(t time.Time) string {
	return strconv.FormatInt(t.UTC().Unix(), 10)
}

func escapePostgresString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
