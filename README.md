# trifle_stats_go (MVP)

Minimal Go implementation of Trifle Stats core functionality for local SQLite usage.

## Scope (MVP)
- Nocturnal time bucketing (`floor`, `add`, `timeline`)
- Key/identifier generation (full/partial/separated)
- Packer (dot-notation pack/unpack)
- SQLite driver with JSON1 (`inc`, `set`, `get`)
- Operations: `Track`, `Assert`, `Values`

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

## Identifier Modes
- `JoinedFull` => `key` (prefix + key + granularity + unix)
- `JoinedPartial` => `key` + `at`
- `JoinedSeparated` => `key` + `granularity` + `at`

## Tests
```bash
go test ./...
```

## Notes
- SQLite uses JSON1 with batched `json_set` updates (avoids parser depth limits).
- Buffering and other drivers are intentionally omitted in MVP.
