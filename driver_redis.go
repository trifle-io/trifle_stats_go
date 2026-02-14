package triflestats

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/redis/go-redis/v9"
)

// RedisDriver implements the Driver interface using Redis hashes.
type RedisDriver struct {
	Client         redis.UniversalClient
	Prefix         string
	Separator      string
	SystemTracking bool
}

// NewRedisDriver creates a Redis driver.
func NewRedisDriver(client redis.UniversalClient, prefix string) *RedisDriver {
	if prefix == "" {
		prefix = "trfl"
	}
	return &RedisDriver{
		Client:         client,
		Prefix:         prefix,
		Separator:      "::",
		SystemTracking: true,
	}
}

func (d *RedisDriver) Description() string {
	return "RedisDriver(J)"
}

// Inc increments numeric values in-place.
func (d *RedisDriver) Inc(keys []Key, values map[string]any) error {
	return d.IncCount(keys, values, 1)
}

// IncCount increments values and records system tracking count.
func (d *RedisDriver) IncCount(keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	if len(keys) == 0 {
		return nil
	}
	if d.Client == nil {
		return fmt.Errorf("redis driver requires Client")
	}

	packed := Pack(values)
	if len(packed) == 0 {
		return nil
	}

	ctx := context.Background()
	for _, key := range keys {
		mainKey := d.joinedKey(key)
		if err := d.incrementPacked(ctx, mainKey, packed); err != nil {
			return err
		}

		if d.SystemTracking {
			systemKey := Key{
				Key:         systemKeyName,
				Granularity: key.Granularity,
				At:          key.At,
				TrackingKey: key.TrackingKey,
			}
			if err := d.incrementPacked(ctx, d.joinedKey(systemKey), systemDataFor(key.SystemTrackingKey(), count)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Set writes values without deleting unspecified fields.
func (d *RedisDriver) Set(keys []Key, values map[string]any) error {
	return d.SetCount(keys, values, 1)
}

// SetCount writes values and records system tracking count.
func (d *RedisDriver) SetCount(keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	if len(keys) == 0 {
		return nil
	}
	if d.Client == nil {
		return fmt.Errorf("redis driver requires Client")
	}

	packed := Pack(values)
	if len(packed) == 0 {
		return nil
	}

	ctx := context.Background()
	fields := toRedisFieldValues(packed)
	for _, key := range keys {
		mainKey := d.joinedKey(key)
		if err := d.Client.HSet(ctx, mainKey, fields...).Err(); err != nil {
			return err
		}

		if d.SystemTracking {
			systemKey := Key{
				Key:         systemKeyName,
				Granularity: key.Granularity,
				At:          key.At,
				TrackingKey: key.TrackingKey,
			}
			if err := d.incrementPacked(ctx, d.joinedKey(systemKey), systemDataFor(key.SystemTrackingKey(), count)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Get fetches values for keys in order.
func (d *RedisDriver) Get(keys []Key) ([]map[string]any, error) {
	if len(keys) == 0 {
		return []map[string]any{}, nil
	}
	if d.Client == nil {
		return nil, fmt.Errorf("redis driver requires Client")
	}

	ctx := context.Background()
	results := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		mainKey := d.joinedKey(key)
		raw, err := d.Client.HGetAll(ctx, mainKey).Result()
		if err != nil {
			return nil, err
		}
		packed := make(map[string]any, len(raw))
		for field, value := range raw {
			packed[field] = parseRedisScalar(value)
		}
		results = append(results, Unpack(packed))
	}
	return results, nil
}

func (d *RedisDriver) joinedKey(key Key) string {
	key.Prefix = d.Prefix
	return key.Join(d.Separator)
}

func (d *RedisDriver) incrementPacked(ctx context.Context, redisKey string, packed map[string]any) error {
	fields := make([]string, 0, len(packed))
	for field := range packed {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	for _, field := range fields {
		value := packed[field]
		delta, ok := toFloat(value)
		if !ok {
			return fmt.Errorf("increment requires numeric value for key %q", field)
		}

		if math.Mod(delta, 1) == 0 {
			if err := d.Client.HIncrBy(ctx, redisKey, field, int64(delta)).Err(); err != nil {
				return err
			}
			continue
		}
		if err := d.Client.HIncrByFloat(ctx, redisKey, field, delta).Err(); err != nil {
			return err
		}
	}
	return nil
}

func toRedisFieldValues(values map[string]any) []any {
	fields := make([]string, 0, len(values))
	for field := range values {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	out := make([]any, 0, len(values)*2)
	for _, field := range fields {
		out = append(out, field, values[field])
	}
	return out
}

func parseRedisScalar(value string) any {
	if number, ok := parseNumericString(value); ok {
		return number
	}
	return value
}
