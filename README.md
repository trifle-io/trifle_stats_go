# trifle_stats_go

Go implementation of Trifle Stats for time-series metrics with pluggable storage drivers.

## Features
- Nocturnal time bucketing (`floor`, `add`, `timeline`)
- Key/identifier generation (`JoinedFull`, `JoinedPartial`, `JoinedSeparated`)
- Packer (dot-notation pack/unpack)
- Drivers:
  - SQLite (`modernc.org/sqlite`)
  - PostgreSQL (`database/sql` with `pgx`)
  - MySQL (`database/sql` with `go-sql-driver/mysql`)
  - Redis (`go-redis/v9`)
  - MongoDB (`mongo-driver`)
- Operations: `Track`, `Assert`, `Values`
- Buffered writes with configurable size/duration/aggregation
- System tracking for per-key write counters (including `Untracked()`)

## Install
```bash
go get github.com/trifle-io/trifle_stats_go
```

## Usage
```go
db, _ := sql.Open("sqlite", "file:stats.db?cache=shared&mode=rwc")
driver := triflestats.NewSQLiteDriver(db, "trifle_stats", triflestats.JoinedFull)
_ = driver.Setup()

cfg := triflestats.DefaultConfig()
cfg.Driver = driver
cfg.TimeZone = "UTC"
cfg.Granularities = []string{"1m", "1h", "1d"}

_ = triflestats.Track(cfg, "event::logs", time.Now(), map[string]any{"count": 1})
_ = triflestats.Assert(cfg, "event::logs", time.Now(), map[string]any{"duration": 2.5})

result, _ := triflestats.Values(cfg, "event::logs", time.Now().Add(-24*time.Hour), time.Now(), "1h", false)
_ = result
```

## Driver Setup

### SQLite
```go
db, _ := sql.Open("sqlite", "file:stats.db?cache=shared&mode=rwc")
driver := triflestats.NewSQLiteDriver(db, "trifle_stats", triflestats.JoinedFull)
_ = driver.Setup()
```

### PostgreSQL
```go
db, _ := sql.Open("pgx", "postgres://postgres:password@localhost:5432/postgres?sslmode=disable")
driver := triflestats.NewPostgresDriver(db, "trifle_stats", triflestats.JoinedSeparated)
_ = driver.Setup()
```

### MySQL
```go
db, _ := sql.Open("mysql", "root:password@tcp(127.0.0.1:3306)/trifle_stats?parseTime=true&loc=UTC")
driver := triflestats.NewMySQLDriver(db, "trifle_stats", triflestats.JoinedSeparated)
_ = driver.Setup()
```

### Redis
```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
driver := triflestats.NewRedisDriver(client, "trfl")
```

### MongoDB
```go
client, _ := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
collection := client.Database("metrics").Collection("trifle_stats")

driver := triflestats.NewMongoDriver(collection, triflestats.JoinedSeparated)
driver.ExpireAfter = 24 * time.Hour
_ = driver.Setup(context.Background())
```

## Identifier Modes
- `JoinedFull` => `key` (prefix + key + granularity + unix)
- `JoinedPartial` => `key` + `at`
- `JoinedSeparated` => `key` + `granularity` + `at`

## Buffering

`DefaultConfig()` enables buffered writes by default. You can tune or disable buffering:

```go
cfg := triflestats.DefaultConfig()
cfg.Driver = driver

cfg.BufferEnabled = true
cfg.BufferDuration = 1 * time.Second
cfg.BufferSize = 256
cfg.BufferAggregate = true
cfg.BufferAsync = true

// Optional lifecycle controls:
_ = cfg.FlushBuffer()
_ = cfg.ShutdownBuffer()
```

## Tests
```bash
go test ./...
```
