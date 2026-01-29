package triflestats

import (
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
	TimezoneLoadError error
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		TimeZone:         "GMT",
		BeginningOfWeek:  time.Monday,
		Granularities:    nil, // nil means default list
		Separator:        "::",
		JoinedIdentifier: JoinedFull,
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
