package triflestats

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

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
func (d *PostgresDriver) Setup() error {
	if d.DB == nil {
		return fmt.Errorf("postgres driver requires DB")
	}

	var query string
	switch d.JoinedIdentifier {
	case JoinedFull:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'::jsonb);`, d.TableName)
	case JoinedPartial:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) NOT NULL, at TIMESTAMPTZ NOT NULL, data JSONB NOT NULL DEFAULT '{}'::jsonb, PRIMARY KEY (key, at));`, d.TableName)
	case JoinedSeparated:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) NOT NULL, granularity VARCHAR(255) NOT NULL, at TIMESTAMPTZ NOT NULL, data JSONB NOT NULL DEFAULT '{}'::jsonb, PRIMARY KEY (key, granularity, at));`, d.TableName)
	default:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key VARCHAR(255) PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'::jsonb);`, d.TableName)
	}

	_, err := d.DB.Exec(query)
	return err
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
func (d *PostgresDriver) Inc(keys []Key, values map[string]any) error {
	return d.IncCount(keys, values, 1)
}

// IncCount increments values and records system tracking count.
func (d *PostgresDriver) IncCount(keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(keys, values, "inc", count)
}

// Set sets provided values without deleting other keys.
func (d *PostgresDriver) Set(keys []Key, values map[string]any) error {
	return d.SetCount(keys, values, 1)
}

// SetCount sets provided values and records system tracking count.
func (d *PostgresDriver) SetCount(keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(keys, values, "set", count)
}

// Get fetches values in key order.
func (d *PostgresDriver) Get(keys []Key) ([]map[string]any, error) {
	if len(keys) == 0 {
		return []map[string]any{}, nil
	}
	if d.DB == nil {
		return nil, fmt.Errorf("postgres driver requires DB")
	}

	results := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		ident, err := d.identifierForKey(key)
		if err != nil {
			return nil, err
		}

		packed, err := d.readPacked(nil, ident)
		if err != nil {
			return nil, err
		}
		results = append(results, Unpack(packed))
	}
	return results, nil
}

func (d *PostgresDriver) writeWithOperation(keys []Key, values map[string]any, op string, count int64) error {
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
		existing, err := d.readPacked(tx, ident)
		if err != nil {
			return err
		}
		merged, err := mergePackedValues(existing, packed, op)
		if err != nil {
			return err
		}
		if err := d.upsertPacked(tx, ident, merged); err != nil {
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
			systemExisting, err := d.readPacked(tx, systemIdent)
			if err != nil {
				return err
			}
			systemMerged, err := mergePackedValues(systemExisting, systemDataFor(key.SystemTrackingKey(), count), "inc")
			if err != nil {
				return err
			}
			if err := d.upsertPacked(tx, systemIdent, systemMerged); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (d *PostgresDriver) identifierForKey(k Key) (identifier, error) {
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
		return identifier{
			columns:   []string{"key", "at"},
			values:    []any{val, *k.At},
			lookupKey: val + "|" + formatAt(*k.At),
		}, nil
	case JoinedSeparated:
		if k.At == nil {
			return identifier{}, fmt.Errorf("separated identifier requires At")
		}
		return identifier{
			columns:   []string{"key", "granularity", "at"},
			values:    []any{k.Key, k.Granularity, *k.At},
			lookupKey: k.Key + "|" + k.Granularity + "|" + formatAt(*k.At),
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

func (d *PostgresDriver) readPacked(tx *sql.Tx, ident identifier) (map[string]any, error) {
	query, args := d.selectQuery(ident)

	var row *sql.Row
	if tx != nil {
		row = tx.QueryRow(query, args...)
	} else {
		row = d.DB.QueryRow(query, args...)
	}

	var raw []byte
	if err := row.Scan(&raw); err != nil {
		if err == sql.ErrNoRows {
			return map[string]any{}, nil
		}
		return nil, err
	}

	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return map[string]any{}, nil
	}
	return decoded, nil
}

func (d *PostgresDriver) upsertPacked(tx *sql.Tx, ident identifier, packed map[string]any) error {
	dataJSON, err := json.Marshal(packed)
	if err != nil {
		return err
	}

	columns := append([]string{}, ident.columns...)
	columns = append(columns, "data")
	conflict := strings.Join(ident.columns, ", ")

	args := append([]any{}, ident.values...)
	args = append(args, string(dataJSON))

	placeholders := make([]string, 0, len(args))
	for idx := range args {
		n := idx + 1
		if idx == len(args)-1 {
			placeholders = append(placeholders, fmt.Sprintf("$%d::jsonb", n))
			continue
		}
		placeholders = append(placeholders, fmt.Sprintf("$%d", n))
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET data = EXCLUDED.data;`,
		d.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		conflict,
	)
	_, err = tx.Exec(query, args...)
	return err
}

func (d *PostgresDriver) selectQuery(ident identifier) (string, []any) {
	parts := make([]string, 0, len(ident.columns))
	args := make([]any, 0, len(ident.values))
	for idx, col := range ident.columns {
		parts = append(parts, fmt.Sprintf("%s = $%d", col, idx+1))
		args = append(args, ident.values[idx])
	}
	query := fmt.Sprintf(`SELECT data FROM %s WHERE %s LIMIT 1;`, d.TableName, strings.Join(parts, " AND "))
	return query, args
}

func mergePackedValues(existing map[string]any, incoming map[string]any, op string) (map[string]any, error) {
	out := cloneMap(existing)

	switch op {
	case "inc":
		for key, value := range incoming {
			delta, ok := toFloat(value)
			if !ok {
				return nil, fmt.Errorf("increment requires numeric value for key %q", key)
			}
			base := 0.0
			if existingValue, ok := toFloat(out[key]); ok {
				base = existingValue
			}
			out[key] = base + delta
		}
	case "set":
		for key, value := range incoming {
			out[key] = value
		}
	default:
		return nil, fmt.Errorf("invalid operation: %s", op)
	}
	return out, nil
}
