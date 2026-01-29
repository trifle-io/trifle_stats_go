package triflestats

import (
	"fmt"
	"time"
)

// Unit represents the time unit in a granularity.
type Unit int

const (
	UnitSecond Unit = iota
	UnitMinute
	UnitHour
	UnitDay
	UnitWeek
	UnitMonth
	UnitQuarter
	UnitYear
)

var unitMap = map[string]Unit{
	"s":  UnitSecond,
	"m":  UnitMinute,
	"h":  UnitHour,
	"d":  UnitDay,
	"w":  UnitWeek,
	"mo": UnitMonth,
	"q":  UnitQuarter,
	"y":  UnitYear,
}

// Parser parses granularity strings like "15m".
type Parser struct {
	String string
	Offset int
	Unit   Unit
}

// ParseGranularity parses strings like "1m", "15m", "1h".
func ParseGranularity(s string) (int, Unit, bool) {
	var offset int
	var unitStr string

	// Simple manual parse to avoid regex overhead.
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch < '0' || ch > '9' {
			offsetPart := s[:i]
			unitStr = s[i:]
			if offsetPart == "" {
				return 0, 0, false
			}
			var err error
			offset, err = atoi(offsetPart)
			if err != nil {
				return 0, 0, false
			}
			break
		}
	}
	if unitStr == "" {
		return 0, 0, false
	}
	unit, ok := unitMap[unitStr]
	if !ok {
		return 0, 0, false
	}
	return offset, unit, true
}

// NewParser constructs a Parser for a granularity string.
func NewParser(s string) *Parser {
	offset, unit, ok := ParseGranularity(s)
	if !ok {
		return &Parser{String: s}
	}
	return &Parser{String: s, Offset: offset, Unit: unit}
}

// Valid indicates if parser parsed successfully.
func (p *Parser) Valid() bool {
	return p != nil && p.Offset > 0
}

// Nocturnal handles time bucketing.
type Nocturnal struct {
	Time   time.Time
	Config *Config
}

// NewNocturnal creates a Nocturnal instance.
func NewNocturnal(t time.Time, cfg *Config) *Nocturnal {
	return &Nocturnal{Time: t, Config: cfg}
}

// Timeline creates a list of bucket boundaries between from and to (inclusive).
func Timeline(from, to time.Time, offset int, unit Unit, cfg *Config) []time.Time {
	list := []time.Time{}
	start := NewNocturnal(from, cfg).Floor(offset, unit)
	end := NewNocturnal(to, cfg).Floor(offset, unit)

	for t := start; !t.After(end); {
		list = append(list, t)
		t = NewNocturnal(t, cfg).Add(offset, unit)
	}
	return list
}

// Add adds an offset of unit to the time.
func (n *Nocturnal) Add(offset int, unit Unit) time.Time {
	t := n.ensureLocation(n.Time)
	if offset == 0 {
		return t
	}

	switch unit {
	case UnitSecond:
		return t.Add(time.Duration(offset) * time.Second)
	case UnitMinute:
		return t.Add(time.Duration(offset) * time.Minute)
	case UnitHour:
		return t.Add(time.Duration(offset) * time.Hour)
	case UnitDay:
		return t.AddDate(0, 0, offset)
	case UnitWeek:
		return t.AddDate(0, 0, offset*7)
	case UnitMonth:
		return addMonths(t, offset)
	case UnitQuarter:
		return addMonths(t, offset*3)
	case UnitYear:
		return addYears(t, offset)
	default:
		panic(fmt.Sprintf("invalid unit: %v", unit))
	}
}

// Floor floors time to bucket boundary for offset/unit.
func (n *Nocturnal) Floor(offset int, unit Unit) time.Time {
	if offset <= 0 {
		panic("offset must be positive")
	}

	t := n.ensureLocation(n.Time)
	loc := t.Location()

	switch unit {
	case UnitSecond:
		floored := (t.Second() / offset) * offset
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), floored, 0, loc)
	case UnitMinute:
		floored := (t.Minute() / offset) * offset
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), floored, 0, 0, loc)
	case UnitHour:
		floored := (t.Hour() / offset) * offset
		return time.Date(t.Year(), t.Month(), t.Day(), floored, 0, 0, 0, loc)
	case UnitDay:
		dayOfYear := t.YearDay() - 1
		flooredDays := (dayOfYear / offset) * offset
		yearStart := time.Date(t.Year(), 1, 1, 0, 0, 0, 0, loc)
		return yearStart.AddDate(0, 0, flooredDays)
	case UnitWeek:
		yearStart := time.Date(t.Year(), 1, 1, 0, 0, 0, 0, loc)
		weekStartOffset := daysIntoWeek(n.configBeginningOfWeek())
		yearStartWday := int(yearStart.Weekday())
		daysToFirst := mod(weekStartOffset-yearStartWday, 7)
		firstWeekStart := yearStart.AddDate(0, 0, daysToFirst)

		if t.Before(firstWeekStart) {
			return yearStart
		}

		diff := t.Sub(firstWeekStart)
		weeksSinceFirst := int(diff.Hours() / (24 * 7))
		flooredWeeks := (weeksSinceFirst / offset) * offset
		return firstWeekStart.AddDate(0, 0, flooredWeeks*7)
	case UnitMonth:
		monthsFromJan := int(t.Month()) - 1
		floored := (monthsFromJan / offset) * offset
		return time.Date(t.Year(), time.Month(floored+1), 1, 0, 0, 0, 0, loc)
	case UnitQuarter:
		currentQuarter := (int(t.Month()) - 1) / 3
		floored := (currentQuarter / offset) * offset
		month := floored*3 + 1
		return time.Date(t.Year(), time.Month(month), 1, 0, 0, 0, 0, loc)
	case UnitYear:
		floored := (t.Year() / offset) * offset
		return time.Date(floored, 1, 1, 0, 0, 0, 0, loc)
	default:
		panic(fmt.Sprintf("invalid unit: %v", unit))
	}
}

func (n *Nocturnal) ensureLocation(t time.Time) time.Time {
	loc := time.UTC
	if n != nil && n.Config != nil {
		loc = n.Config.Location()
	}
	return t.In(loc)
}

func (n *Nocturnal) configBeginningOfWeek() time.Weekday {
	if n == nil || n.Config == nil {
		return time.Monday
	}
	return n.Config.BeginningOfWeek
}

func daysIntoWeek(day time.Weekday) int {
	// Match Ruby: Sunday=0, Monday=1, ..., Saturday=6
	switch day {
	case time.Sunday:
		return 0
	case time.Monday:
		return 1
	case time.Tuesday:
		return 2
	case time.Wednesday:
		return 3
	case time.Thursday:
		return 4
	case time.Friday:
		return 5
	case time.Saturday:
		return 6
	default:
		return 1
	}
}

func mod(a, b int) int {
	r := a % b
	if r < 0 {
		return r + b
	}
	return r
}

func addMonths(t time.Time, months int) time.Time {
	year, month := t.Year(), int(t.Month())
	total := (year*12 + (month - 1)) + months
	newYear := total / 12
	newMonth := total%12 + 1
	day := t.Day()
	maxDay := daysInMonth(newYear, time.Month(newMonth))
	if day > maxDay {
		day = maxDay
	}
	return time.Date(newYear, time.Month(newMonth), day, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
}

func addYears(t time.Time, years int) time.Time {
	newYear := t.Year() + years
	day := t.Day()
	maxDay := daysInMonth(newYear, t.Month())
	if day > maxDay {
		day = maxDay
	}
	return time.Date(newYear, t.Month(), day, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
}

func daysInMonth(year int, month time.Month) int {
	// The zero day of next month is the last day of current month.
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func atoi(s string) (int, error) {
	var n int
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid number")
		}
		n = n*10 + int(ch-'0')
	}
	return n, nil
}
