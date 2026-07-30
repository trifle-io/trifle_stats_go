package triflestats

import (
	"fmt"
	"sort"
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
		candidate := NewNocturnal(t, cfg).Add(offset, unit)
		t = NewNocturnal(candidate, cfg).Floor(offset, unit)
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
		return addCalendarDays(t, offset)
	case UnitWeek:
		return addCalendarDays(t, offset*7)
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

	switch unit {
	case UnitSecond:
		floored := (t.Second() / offset) * offset
		elapsed := time.Duration(t.Second()-floored)*time.Second + time.Duration(t.Nanosecond())
		return t.Add(-elapsed)
	case UnitMinute:
		floored := (t.Minute() / offset) * offset
		elapsed := time.Duration(t.Minute()-floored)*time.Minute +
			time.Duration(t.Second())*time.Second +
			time.Duration(t.Nanosecond())
		return t.Add(-elapsed)
	case UnitHour:
		dayStart := resolveLocal(t, t.Year(), t.Month(), t.Day(), 0, 0, 0, 0)
		segment := time.Duration(offset) * time.Hour
		elapsed := t.Sub(dayStart)
		return dayStart.Add((elapsed / segment) * segment)
	case UnitDay:
		dayOfYear := t.YearDay() - 1
		flooredDays := (dayOfYear / offset) * offset
		date := time.Date(t.Year(), 1, 1+flooredDays, 0, 0, 0, 0, time.UTC)
		return resolveLocal(t, date.Year(), date.Month(), date.Day(), 0, 0, 0, 0)
	case UnitWeek:
		yearStart := time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		weekStartOffset := daysIntoWeek(n.configBeginningOfWeek())
		yearStartWday := int(yearStart.Weekday())
		daysToFirst := mod(weekStartOffset-yearStartWday, 7)
		firstWeekStart := yearStart.AddDate(0, 0, daysToFirst)

		currentDate := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		if currentDate.Before(firstWeekStart) {
			return resolveLocal(t, yearStart.Year(), yearStart.Month(), yearStart.Day(), 0, 0, 0, 0)
		}

		weeksSinceFirst := daysBetween(firstWeekStart, currentDate) / 7
		flooredWeeks := (weeksSinceFirst / offset) * offset
		date := firstWeekStart.AddDate(0, 0, flooredWeeks*7)
		return resolveLocal(t, date.Year(), date.Month(), date.Day(), 0, 0, 0, 0)
	case UnitMonth:
		monthsFromJan := int(t.Month()) - 1
		floored := (monthsFromJan / offset) * offset
		return resolveLocal(t, t.Year(), time.Month(floored+1), 1, 0, 0, 0, 0)
	case UnitQuarter:
		currentQuarter := (int(t.Month()) - 1) / 3
		floored := (currentQuarter / offset) * offset
		month := floored*3 + 1
		return resolveLocal(t, t.Year(), time.Month(month), 1, 0, 0, 0, 0)
	case UnitYear:
		floored := (t.Year() / offset) * offset
		return resolveLocal(t, floored, 1, 1, 0, 0, 0, 0)
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
	return resolveLocal(t, newYear, time.Month(newMonth), day, t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
}

func addYears(t time.Time, years int) time.Time {
	newYear := t.Year() + years
	day := t.Day()
	maxDay := daysInMonth(newYear, t.Month())
	if day > maxDay {
		day = maxDay
	}
	return resolveLocal(t, newYear, t.Month(), day, t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
}

func addCalendarDays(t time.Time, days int) time.Time {
	date := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, days)
	return resolveLocal(t, date.Year(), date.Month(), date.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
}

type zonePeriod struct {
	start  time.Time
	offset int
}

func resolveLocal(source time.Time, year int, month time.Month, day, hour, minute, second, nanosecond int) time.Time {
	loc := source.Location()
	wall := time.Date(year, month, day, hour, minute, second, nanosecond, time.UTC)
	periods := zonePeriodsAround(wall, loc)
	candidates := localCandidates(wall, loc, periods)
	_, sourceOffset := source.Zone()

	if len(candidates) > 0 {
		for _, candidate := range candidates {
			_, candidateOffset := candidate.Zone()
			if candidateOffset == sourceOffset {
				return candidate
			}
		}
		return candidates[0]
	}

	for i := 1; i < len(periods); i++ {
		previous := periods[i-1]
		next := periods[i]
		if next.offset <= previous.offset {
			continue
		}
		gapStart := next.start.Add(time.Duration(previous.offset) * time.Second)
		gapEnd := next.start.Add(time.Duration(next.offset) * time.Second)
		if !wall.Before(gapStart) && wall.Before(gapEnd) {
			gap := time.Duration(next.offset-previous.offset) * time.Second
			shifted := wall.Add(gap)
			return resolveLocal(
				source,
				shifted.Year(), shifted.Month(), shifted.Day(),
				shifted.Hour(), shifted.Minute(), shifted.Second(), shifted.Nanosecond(),
			)
		}
	}

	return time.Date(year, month, day, hour, minute, second, nanosecond, loc)
}

func zonePeriodsAround(wall time.Time, loc *time.Location) []zonePeriod {
	lower := wall.Add(-48 * time.Hour)
	upper := wall.Add(48 * time.Hour)
	cursor := lower
	periods := make([]zonePeriod, 0, 4)

	for !cursor.After(upper) {
		local := cursor.In(loc)
		_, offset := local.Zone()
		start, end := local.ZoneBounds()
		periods = append(periods, zonePeriod{start: start, offset: offset})
		if end.IsZero() || !end.Before(upper) {
			break
		}
		cursor = end.Add(time.Nanosecond)
	}
	return periods
}

func localCandidates(wall time.Time, loc *time.Location, periods []zonePeriod) []time.Time {
	seen := map[int]bool{}
	candidates := make([]time.Time, 0, 2)

	for _, period := range periods {
		if seen[period.offset] {
			continue
		}
		seen[period.offset] = true
		candidate := wall.Add(-time.Duration(period.offset) * time.Second).In(loc)
		if sameWallTime(candidate, wall) {
			candidates = append(candidates, candidate)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Before(candidates[j]) })
	return candidates
}

func sameWallTime(local, wall time.Time) bool {
	return local.Year() == wall.Year() &&
		local.Month() == wall.Month() &&
		local.Day() == wall.Day() &&
		local.Hour() == wall.Hour() &&
		local.Minute() == wall.Minute() &&
		local.Second() == wall.Second() &&
		local.Nanosecond() == wall.Nanosecond()
}

func daysBetween(from, to time.Time) int {
	return int(to.Sub(from) / (24 * time.Hour))
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
