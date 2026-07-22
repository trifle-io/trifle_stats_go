# Trifle Stats (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/trifle-io/trifle_stats_go.svg)](https://pkg.go.dev/github.com/trifle-io/trifle_stats_go)

Time-series metrics for Go. Track anything (signups, revenue, job durations) using the database you already have. No InfluxDB. No TimescaleDB. Just one call and your existing PostgreSQL, MySQL, Redis, MongoDB, or SQLite.

Part of the [Trifle](https://trifle.io) ecosystem. Also available in [Ruby](https://github.com/trifle-io/trifle-stats) and [Elixir](https://github.com/trifle-io/trifle_stats).

## Quick Start

```bash
go get github.com/trifle-io/trifle_stats_go
```

```go
import triflestats "github.com/trifle-io/trifle_stats_go"

ctx := context.Background()

// Open your existing database
db, _ := sql.Open("pgx", "postgres://localhost:5432/myapp?sslmode=disable")
driver := triflestats.NewPostgresDriver(db, "trifle_stats", triflestats.JoinedSeparated)
_ = driver.Setup(ctx)

// Configure
cfg := triflestats.DefaultConfig()
cfg.Driver = driver
cfg.Granularities = []string{"1h", "1d", "1w", "1mo"}

// Track
_ = triflestats.Track(ctx, cfg, "orders", time.Now(), map[string]any{
    "count":   1,
    "revenue": 4990,
})

// Flush buffered writes before your process exits (see Buffering below)
defer cfg.ShutdownBuffer()

// Query
result, _ := triflestats.Values(ctx, cfg, "orders", time.Now().Add(-7*24*time.Hour), time.Now(), "1d", false)

// Process with Series
series := triflestats.SeriesFromResult(result)
series, _ = series.TransformExpression([]string{"revenue", "count"}, "a / b", "avg_order")
timeline := series.FormatTimeline("avg_order", 1, nil)
_ = timeline
```

## Drivers

| Driver | Backend | Best for |
|--------|---------|----------|
| **API** | Trifle Cloud Projects | Hosted metrics without your own database |
| **PostgreSQL** | JSONB upsert | Most production apps |
| **MySQL** | JSON column | MySQL shops |
| **SQLite** | JSON1 extension | Single-binary apps, dev/test |
| **Redis** | Hash increment | High-throughput counters |
| **MongoDB** | Document upsert | Document-oriented stacks |
| **Process** | In-memory maps | Tests, single-process tools |

### Driver Setup

```go
// PostgreSQL
db, _ := sql.Open("pgx", "postgres://localhost:5432/myapp?sslmode=disable")
driver := triflestats.NewPostgresDriver(db, "trifle_stats", triflestats.JoinedSeparated)

// MySQL
db, _ := sql.Open("mysql", "root:password@tcp(127.0.0.1:3306)/myapp?parseTime=true&loc=UTC")
driver := triflestats.NewMySQLDriver(db, "trifle_stats", triflestats.JoinedSeparated)

// SQLite
db, _ := sql.Open("sqlite", "file:stats.db?cache=shared&mode=rwc")
driver := triflestats.NewSQLiteDriver(db, "trifle_stats", triflestats.JoinedFull)

// Redis
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
driver := triflestats.NewRedisDriver(client, "trfl")

// MongoDB
client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
collection := client.Database("metrics").Collection("trifle_stats")
driver := triflestats.NewMongoDriver(collection, triflestats.JoinedSeparated)

// Process (in-memory, for tests and single-process tools)
driver := triflestats.NewProcessDriver()
```

## Features

- **Track, Assert, Values.** Increment counters, set absolute values, query time ranges.
- **Beam, Scan.** Store and retrieve the latest status payload per key (separated mode on PostgreSQL/MySQL/SQLite/MongoDB).
- **Buffered writes.** Configurable in-memory buffer with size/duration/aggregation controls.
- **Dynamic granularities.** Use any interval like `1m`, `10m`, `1h`, `6h`, `1d`, `1w`, `1mo`, `1q`, `1y`.
- **Nested values.** Dot-notation packing for hierarchical data.
- **Expression-based derived metrics.** Build averages, ratios, and custom formulas with one transponder.
- **Data compatible.** Same storage format as the Ruby and Elixir implementations.

## Status (Beam/Scan)

In separated mode, `Beam` stores the latest status payload for a key and `Scan`
retrieves it. Both bypass the write buffer and use the driver directly:

```go
_ = triflestats.Beam(ctx, cfg, "jobs::sync", time.Now(), map[string]any{"state": "running"})

status, _ := triflestats.Scan(ctx, cfg, "jobs::sync")
if status.Found {
    fmt.Println(status.At, status.Values)
}
```

Joined/partial modes and the Redis driver treat Beam as a no-op and Scan
reports `Found == false`, mirroring the Ruby implementation. The in-memory
Process driver keeps the latest status per key.

## Buffering

Buffered writes are enabled by default. Tune or disable:

```go
cfg := triflestats.DefaultConfig()
cfg.BufferEnabled = true
cfg.BufferDuration = 1 * time.Second
cfg.BufferSize = 256
cfg.BufferAggregate = true
cfg.BufferAsync = true
```

**Flush before exit.** Unlike the Ruby and Elixir implementations, which
install `at_exit`/VM shutdown hooks, Go has no reliable process-exit hook —
you must flush the buffer explicitly before your process exits or the most
recent writes are lost:

```go
defer cfg.ShutdownBuffer() // flush pending writes and stop the background worker
// or, to flush without stopping the worker:
_ = cfg.FlushBuffer()
```

## System tracking

Every driver records internal usage stats under a `__system__key__` row
(operation counts per tracked key), matching the Ruby/Elixir implementations.
This doubles write volume — one extra write per tracked key. Disable it on the
driver if you don't need it:

```go
driver := triflestats.NewPostgresDriver(db, "trifle_stats", triflestats.JoinedSeparated)
driver.SystemTracking = false
```

## Documentation

Full guides, API reference, and examples at **[docs.trifle.io/trifle-stats-go](https://docs.trifle.io/trifle-stats-go)**

## Trifle Ecosystem

Trifle Stats is the tracking layer. The ecosystem grows with you:

| Component | What it does |
|-----------|-------------|
| **[Trifle App](https://trifle.io/product/app)** | Dashboards, alerts, scheduled reports, AI-powered chat. Cloud or self-hosted. |
| **[Trifle CLI](https://github.com/trifle-io/trifle-cli)** | Query and push metrics from the terminal. MCP server mode for AI agents. |
| **[Trifle::Stats (Ruby)](https://github.com/trifle-io/trifle-stats)** | Ruby implementation with the same API and storage format. |
| **[Trifle.Stats (Elixir)](https://github.com/trifle-io/trifle_stats)** | Elixir implementation with the same API and storage format. |

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/trifle-io/trifle_stats_go.

## License

Available under the terms of the [MIT License](https://opensource.org/licenses/MIT).
