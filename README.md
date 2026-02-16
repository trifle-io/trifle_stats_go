# Trifle Stats (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/trifle-io/trifle_stats_go.svg)](https://pkg.go.dev/github.com/trifle-io/trifle_stats_go)

Time-series metrics for Go. Track anything — signups, revenue, job durations — using the database you already have. No InfluxDB. No TimescaleDB. Just one call and your existing PostgreSQL, MySQL, Redis, MongoDB, or SQLite.

Part of the [Trifle](https://trifle.io) ecosystem. Also available in [Ruby](https://github.com/trifle-io/trifle-stats) and [Elixir](https://github.com/trifle-io/trifle_stats).

## Quick Start

```bash
go get github.com/trifle-io/trifle_stats_go
```

```go
import triflestats "github.com/trifle-io/trifle_stats_go"

// Open your existing database
db, _ := sql.Open("pgx", "postgres://localhost:5432/myapp?sslmode=disable")
driver := triflestats.NewPostgresDriver(db, "trifle_stats", triflestats.JoinedSeparated)
_ = driver.Setup()

// Configure
cfg := triflestats.DefaultConfig()
cfg.Driver = driver
cfg.Granularities = []string{"1h", "1d", "1w", "1mo"}

// Track
_ = triflestats.Track(cfg, "orders", time.Now(), map[string]any{
    "count":   1,
    "revenue": 4990,
})

// Query
result, _ := triflestats.Values(cfg, "orders", time.Now().Add(-7*24*time.Hour), time.Now(), "1d", false)
```

## Drivers

| Driver | Backend | Best for |
|--------|---------|----------|
| **PostgreSQL** | JSONB upsert | Most production apps |
| **MySQL** | JSON column | MySQL shops |
| **SQLite** | JSON1 extension | Single-binary apps, dev/test |
| **Redis** | Hash increment | High-throughput counters |
| **MongoDB** | Document upsert | Document-oriented stacks |

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
```

## Features

- **Track, Assert, Values** — Increment counters, set absolute values, query time ranges
- **Buffered writes** — Configurable in-memory buffer with size/duration/aggregation controls
- **Multiple granularities** — minute, hour, day, week, month, quarter, year
- **Nested values** — Dot-notation packing for hierarchical data
- **Data compatible** — Same storage format as the Ruby and Elixir implementations

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

## Trifle Ecosystem

Trifle Stats is the tracking layer. The ecosystem grows with you:

| Component | What it does |
|-----------|-------------|
| **[Trifle App](https://trifle.io/product-app)** | Dashboards, alerts, scheduled reports, AI-powered chat. Cloud or self-hosted. |
| **[Trifle CLI](https://github.com/trifle-io/trifle-cli)** | Query and push metrics from the terminal. MCP server mode for AI agents. |
| **[Trifle::Stats (Ruby)](https://github.com/trifle-io/trifle-stats)** | Ruby implementation with the same API and storage format. |
| **[Trifle.Stats (Elixir)](https://github.com/trifle-io/trifle_stats)** | Elixir implementation with the same API and storage format. |

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/trifle-io/trifle_stats_go.

## License

Available under the terms of the [MIT License](https://opensource.org/licenses/MIT).
