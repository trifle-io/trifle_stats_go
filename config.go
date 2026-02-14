package triflestats

import (
	"sync"
	"time"
)

// DefaultGranularities matches Ruby/Elixir defaults.
var DefaultGranularities = []string{"1m", "1h", "1d", "1w", "1mo", "1q", "1y"}

// Config holds global configuration for Trifle Stats.
type Config struct {
	Driver            Driver
	TimeZone          string
	BeginningOfWeek   time.Weekday
	Granularities     []string
	Separator         string
	JoinedIdentifier  JoinedIdentifier
	BufferEnabled     bool
	BufferDuration    time.Duration
	BufferSize        int
	BufferAggregate   bool
	BufferAsync       bool
	TimezoneLoadError error

	bufferMu sync.Mutex
	storage  WriteStorage
	buffer   *Buffer
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		TimeZone:         "GMT",
		BeginningOfWeek:  time.Monday,
		Granularities:    nil, // nil means default list
		Separator:        "::",
		JoinedIdentifier: JoinedFull,
		BufferEnabled:    true,
		BufferDuration:   time.Second,
		BufferSize:       256,
		BufferAggregate:  true,
		BufferAsync:      true,
	}
}

// Location resolves the configured time zone, defaulting to UTC on error.
func (c *Config) Location() *time.Location {
	if c == nil || c.TimeZone == "" {
		return time.UTC
	}
	loc, err := time.LoadLocation(c.TimeZone)
	if err != nil {
		if c != nil {
			c.TimezoneLoadError = err
		}
		return time.UTC
	}
	return loc
}

// EffectiveGranularities returns the configured granularities filtered by validity.
// Nil Granularities means default list. Empty list remains empty.
func (c *Config) EffectiveGranularities() []string {
	if c == nil {
		return DefaultGranularities
	}

	base := c.Granularities
	if base == nil {
		base = DefaultGranularities
	}

	out := make([]string, 0, len(base))
	seen := map[string]struct{}{}
	for _, g := range base {
		if _, ok := seen[g]; ok {
			continue
		}
		_, _, ok := ParseGranularity(g)
		if !ok {
			continue
		}
		seen[g] = struct{}{}
		out = append(out, g)
	}
	return out
}

// Storage returns the configured write storage backend (buffer or raw driver).
func (c *Config) Storage() WriteStorage {
	if c == nil {
		return nil
	}

	c.bufferMu.Lock()
	defer c.bufferMu.Unlock()

	if !c.BufferEnabled {
		c.shutdownBufferLocked()
		return c.Driver
	}
	if c.Driver == nil {
		c.shutdownBufferLocked()
		return nil
	}

	if c.buffer != nil && c.buffer.matches(c.Driver, c.BufferDuration, c.BufferSize, c.BufferAggregate, c.BufferAsync) {
		return c.storage
	}

	c.shutdownBufferLocked()
	c.buffer = NewBuffer(c.Driver, BufferOptions{
		Duration:  c.BufferDuration,
		Size:      c.BufferSize,
		Aggregate: c.BufferAggregate,
		Async:     c.BufferAsync,
	})
	c.storage = c.buffer
	return c.storage
}

// FlushBuffer flushes pending buffered actions.
func (c *Config) FlushBuffer() error {
	if c == nil {
		return nil
	}

	c.bufferMu.Lock()
	buffer := c.buffer
	c.bufferMu.Unlock()
	if buffer == nil {
		return nil
	}
	return buffer.Flush()
}

// ShutdownBuffer flushes and stops the buffer worker.
func (c *Config) ShutdownBuffer() error {
	if c == nil {
		return nil
	}

	c.bufferMu.Lock()
	buffer := c.buffer
	c.buffer = nil
	c.storage = nil
	c.bufferMu.Unlock()
	if buffer == nil {
		return nil
	}
	return buffer.Shutdown()
}

func (c *Config) shutdownBufferLocked() {
	if c.buffer == nil {
		c.storage = nil
		return
	}
	_ = c.buffer.Shutdown()
	c.buffer = nil
	c.storage = nil
}
