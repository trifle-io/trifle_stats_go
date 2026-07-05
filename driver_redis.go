package triflestats

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

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
func (d *RedisDriver) Inc(ctx context.Context, keys []Key, values map[string]any) error {
	return d.IncCount(ctx, keys, values, 1)
}

// IncCount increments values and records system tracking count.
func (d *RedisDriver) IncCount(ctx context.Context, keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	if ctx == nil {
		ctx = context.Background()
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

	_, err := d.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			mainKey := d.joinedKey(key)
			if err := incrementPackedPipelined(ctx, pipe, mainKey, packed); err != nil {
				return err
			}
			if err := d.trackSystemData(ctx, pipe, key, count); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Set writes values without deleting unspecified fields.
func (d *RedisDriver) Set(ctx context.Context, keys []Key, values map[string]any) error {
	return d.SetCount(ctx, keys, values, 1)
}

// SetCount writes values and records system tracking count.
func (d *RedisDriver) SetCount(ctx context.Context, keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	if ctx == nil {
		ctx = context.Background()
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

	fields := toRedisFieldValues(packed)
	_, err := d.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			mainKey := d.joinedKey(key)
			pipe.HSet(ctx, mainKey, fields...)
			if err := d.trackSystemData(ctx, pipe, key, count); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Get fetches values for keys in order using a single pipelined round trip.
func (d *RedisDriver) Get(ctx context.Context, keys []Key) ([]map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return []map[string]any{}, nil
	}
	if d.Client == nil {
		return nil, fmt.Errorf("redis driver requires Client")
	}

	cmds := make([]*redis.MapStringStringCmd, 0, len(keys))
	_, err := d.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			cmds = append(cmds, pipe.HGetAll(ctx, d.joinedKey(key)))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	results := make([]map[string]any, 0, len(keys))
	for _, cmd := range cmds {
		raw, err := cmd.Result()
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

// Ping is a no-op for Redis, mirroring the Ruby driver.
func (d *RedisDriver) Ping(ctx context.Context, key Key, values map[string]any) error {
	return nil
}

// Scan is a no-op for Redis, mirroring the Ruby driver.
func (d *RedisDriver) Scan(ctx context.Context, key Key) (time.Time, map[string]any, bool, error) {
	return time.Time{}, nil, false, nil
}

func (d *RedisDriver) trackSystemData(ctx context.Context, pipe redis.Pipeliner, key Key, count int64) error {
	if !d.SystemTracking {
		return nil
	}
	systemKey := Key{
		Key:         systemKeyName,
		Granularity: key.Granularity,
		At:          key.At,
	}
	return incrementPackedPipelined(ctx, pipe, d.joinedKey(systemKey), systemDataFor(key.SystemTrackingKey(), count))
}

func (d *RedisDriver) joinedKey(key Key) string {
	key.Prefix = d.Prefix
	return key.Join(d.Separator)
}

func incrementPackedPipelined(ctx context.Context, pipe redis.Pipeliner, redisKey string, packed map[string]any) error {
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
			pipe.HIncrBy(ctx, redisKey, field, int64(delta))
			continue
		}
		pipe.HIncrByFloat(ctx, redisKey, field, delta)
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
