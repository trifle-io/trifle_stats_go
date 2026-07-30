package triflestats

import (
	"reflect"
	"testing"
	"time"
)

func TestParser(t *testing.T) {
	p := NewParser("15m")
	if !p.Valid() {
		t.Fatalf("expected parser to be valid")
	}
	if p.Offset != 15 || p.Unit != UnitMinute {
		t.Fatalf("unexpected parser result: %+v", p)
	}

	p = NewParser("invalid")
	if p.Valid() {
		t.Fatalf("expected invalid parser")
	}
}

func TestNocturnalFloorAddTimeline(t *testing.T) {
	loc := time.UTC
	cfg := DefaultConfig()
	cfg.TimeZone = "UTC"

	at := time.Date(2025, 1, 15, 10, 37, 45, 0, loc)
	n := NewNocturnal(at, cfg)

	floored := n.Floor(15, UnitMinute)
	expected := time.Date(2025, 1, 15, 10, 30, 0, 0, loc)
	if !floored.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, floored)
	}

	added := n.Add(1, UnitHour)
	if added.Hour() != 11 {
		t.Fatalf("expected hour 11, got %d", added.Hour())
	}

	timeline := Timeline(
		time.Date(2025, 1, 15, 10, 37, 0, 0, loc),
		time.Date(2025, 1, 15, 11, 05, 0, 0, loc),
		15,
		UnitMinute,
		cfg,
	)
	expectedTimeline := []time.Time{
		time.Date(2025, 1, 15, 10, 30, 0, 0, loc),
		time.Date(2025, 1, 15, 10, 45, 0, 0, loc),
		time.Date(2025, 1, 15, 11, 0, 0, 0, loc),
	}
	if !reflect.DeepEqual(timeline, expectedTimeline) {
		t.Fatalf("unexpected timeline: %+v", timeline)
	}

	dynamicTimeline := Timeline(
		time.Date(2025, 1, 15, 10, 37, 0, 0, loc),
		time.Date(2025, 1, 15, 11, 10, 0, 0, loc),
		33,
		UnitMinute,
		cfg,
	)
	expectedDynamic := []time.Time{
		time.Date(2025, 1, 15, 10, 33, 0, 0, loc),
		time.Date(2025, 1, 15, 11, 0, 0, 0, loc),
	}
	if !reflect.DeepEqual(dynamicTimeline, expectedDynamic) {
		t.Fatalf("unexpected dynamic timeline: %+v", dynamicTimeline)
	}
}

func TestNocturnalWeekFloor(t *testing.T) {
	loc := time.UTC
	cfg := DefaultConfig()
	cfg.TimeZone = "UTC"
	cfg.BeginningOfWeek = time.Monday

	at := time.Date(2025, 1, 2, 12, 0, 0, 0, loc) // Thu
	n := NewNocturnal(at, cfg)
	floored := n.Floor(1, UnitWeek)

	expected := time.Date(2025, 1, 1, 0, 0, 0, 0, loc) // year start (before first week boundary)
	if !floored.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, floored)
	}
}

func TestNocturnalAddMonths(t *testing.T) {
	loc := time.UTC
	cfg := DefaultConfig()
	cfg.TimeZone = "UTC"

	at := time.Date(2025, 1, 31, 10, 0, 0, 0, loc)
	n := NewNocturnal(at, cfg)
	added := n.Add(1, UnitMonth)
	expected := time.Date(2025, 2, 28, 10, 0, 0, 0, loc)
	if !added.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, added)
	}
}

func TestNocturnalDSTDailyTimeline(t *testing.T) {
	loc := mustLocation(t, "Europe/Bratislava")
	cfg := DefaultConfig()
	cfg.TimeZone = loc.String()

	timeline := Timeline(
		time.Date(2026, 1, 1, 0, 0, 0, 0, loc),
		time.Date(2026, 7, 20, 23, 59, 59, 0, loc),
		1,
		UnitDay,
		cfg,
	)
	tracked := NewNocturnal(time.Date(2026, 7, 20, 12, 0, 0, 0, loc), cfg).Floor(1, UnitDay)

	if len(timeline) != 201 {
		t.Fatalf("expected 201 daily buckets, got %d", len(timeline))
	}
	if expected := time.Date(2026, 3, 30, 0, 0, 0, 0, loc); !timeline[88].Equal(expected) {
		t.Fatalf("expected bucket 88 to be %v, got %v", expected, timeline[88])
	}
	if expected := time.Date(2026, 7, 20, 0, 0, 0, 0, loc); !timeline[len(timeline)-1].Equal(expected) {
		t.Fatalf("expected final bucket to be %v, got %v", expected, timeline[len(timeline)-1])
	}
	if !containsTime(timeline, tracked) {
		t.Fatalf("expected timeline to contain tracked bucket %v", tracked)
	}
	for _, at := range timeline {
		if at.Hour() != 0 {
			t.Fatalf("expected local midnight, got %v", at)
		}
	}
}

func TestNocturnalDSTHourlyTimeline(t *testing.T) {
	loc := mustLocation(t, "Europe/Bratislava")
	cfg := DefaultConfig()
	cfg.TimeZone = loc.String()

	fall := Timeline(
		time.Date(2026, 10, 25, 0, 0, 0, 0, loc),
		time.Date(2026, 10, 25, 4, 0, 0, 0, loc),
		1,
		UnitHour,
		cfg,
	)
	expectedFall := [][2]int{{0, 7200}, {1, 7200}, {2, 7200}, {2, 3600}, {3, 3600}, {4, 3600}}
	if actual := wallHoursAndOffsets(fall); !reflect.DeepEqual(actual, expectedFall) {
		t.Fatalf("unexpected fallback timeline: %v", actual)
	}

	spring := Timeline(
		time.Date(2026, 3, 29, 0, 0, 0, 0, loc),
		time.Date(2026, 3, 29, 5, 0, 0, 0, loc),
		1,
		UnitHour,
		cfg,
	)
	expectedSpring := [][2]int{{0, 3600}, {1, 3600}, {3, 7200}, {4, 7200}, {5, 7200}}
	if actual := wallHoursAndOffsets(spring); !reflect.DeepEqual(actual, expectedSpring) {
		t.Fatalf("unexpected spring timeline: %v", actual)
	}

	multiHour := Timeline(
		time.Date(2026, 10, 25, 0, 0, 0, 0, loc),
		time.Date(2026, 10, 26, 0, 0, 0, 0, loc),
		6,
		UnitHour,
		cfg,
	)
	expectedMultiHour := [][3]int{
		{25, 0, 7200},
		{25, 5, 3600},
		{25, 11, 3600},
		{25, 17, 3600},
		{25, 23, 3600},
		{26, 0, 3600},
	}
	if actual := wallDaysHoursAndOffsets(multiHour); !reflect.DeepEqual(actual, expectedMultiHour) {
		t.Fatalf("unexpected multi-hour timeline: %v", actual)
	}
	for _, at := range multiHour {
		if floored := NewNocturnal(at, cfg).Floor(6, UnitHour); !floored.Equal(at) {
			t.Fatalf("timeline bucket %v floors to %v", at, floored)
		}
	}
}

func TestNocturnalDSTCalendarBoundaries(t *testing.T) {
	loc := mustLocation(t, "Europe/Bratislava")
	cfg := DefaultConfig()
	cfg.TimeZone = loc.String()
	cfg.BeginningOfWeek = time.Monday

	week := time.Date(2026, 4, 6, 0, 0, 0, 0, loc)
	if actual := NewNocturnal(week, cfg).Floor(1, UnitWeek); !actual.Equal(week) {
		t.Fatalf("expected exact weekly boundary %v, got %v", week, actual)
	}
	twoWeekBoundary := time.Date(2026, 3, 30, 0, 0, 0, 0, loc)
	if actual := NewNocturnal(twoWeekBoundary, cfg).Floor(2, UnitWeek); !actual.Equal(twoWeekBoundary) {
		t.Fatalf("expected exact two-week boundary %v, got %v", twoWeekBoundary, actual)
	}

	summer := time.Date(2026, 7, 20, 12, 0, 0, 0, loc)
	expectedYear := time.Date(2026, 1, 1, 0, 0, 0, 0, loc)
	if actual := NewNocturnal(summer, cfg).Floor(1, UnitYear); !actual.Equal(expectedYear) {
		t.Fatalf("expected yearly boundary %v, got %v", expectedYear, actual)
	}
	expectedQuarter := time.Date(2026, 7, 1, 0, 0, 0, 0, loc)
	if actual := NewNocturnal(summer, cfg).Floor(1, UnitQuarter); !actual.Equal(expectedQuarter) {
		t.Fatalf("expected quarterly boundary %v, got %v", expectedQuarter, actual)
	}

	january := time.Date(2026, 1, 31, 12, 0, 0, 0, loc)
	expectedJuly := time.Date(2026, 7, 31, 12, 0, 0, 0, loc)
	if actual := NewNocturnal(january, cfg).Add(6, UnitMonth); !actual.Equal(expectedJuly) {
		t.Fatalf("expected calendar month result %v, got %v", expectedJuly, actual)
	}
	expectedNovember := time.Date(2025, 11, 30, 12, 0, 0, 0, loc)
	if actual := NewNocturnal(january, cfg).Add(-2, UnitMonth); !actual.Equal(expectedNovember) {
		t.Fatalf("expected negative calendar month result %v, got %v", expectedNovember, actual)
	}
}

func TestNocturnalDSTResolvesGapAndAmbiguity(t *testing.T) {
	loc := mustLocation(t, "Europe/Bratislava")
	cfg := DefaultConfig()
	cfg.TimeZone = loc.String()

	beforeGap := time.Date(2026, 3, 28, 2, 30, 0, 0, loc)
	afterGap := NewNocturnal(beforeGap, cfg).Add(1, UnitDay)
	assertWallTimeAndOffset(t, afterGap, 2026, time.March, 29, 3, 30, 7200)

	summerSource := time.Date(2026, 10, 24, 2, 30, 0, 0, loc)
	firstOccurrence := NewNocturnal(summerSource, cfg).Add(1, UnitDay)
	assertWallTimeAndOffset(t, firstOccurrence, 2026, time.October, 25, 2, 30, 7200)

	winterSource := time.Date(2026, 10, 26, 2, 30, 0, 0, loc)
	secondOccurrence := NewNocturnal(winterSource, cfg).Add(-1, UnitDay)
	assertWallTimeAndOffset(t, secondOccurrence, 2026, time.October, 25, 2, 30, 3600)
}

func TestNocturnalDSTUsesActualTransitionGap(t *testing.T) {
	loc := mustLocation(t, "Australia/Lord_Howe")
	cfg := DefaultConfig()
	cfg.TimeZone = loc.String()

	source := time.Date(2026, 10, 3, 2, 15, 0, 0, loc)
	actual := NewNocturnal(source, cfg).Add(1, UnitDay)

	assertWallTimeAndOffset(t, actual, 2026, time.October, 4, 2, 45, 39600)
}

func mustLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("load location %q: %v", name, err)
	}
	return loc
}

func containsTime(list []time.Time, target time.Time) bool {
	for _, item := range list {
		if item.Equal(target) {
			return true
		}
	}
	return false
}

func wallHoursAndOffsets(list []time.Time) [][2]int {
	result := make([][2]int, 0, len(list))
	for _, item := range list {
		_, offset := item.Zone()
		result = append(result, [2]int{item.Hour(), offset})
	}
	return result
}

func wallDaysHoursAndOffsets(list []time.Time) [][3]int {
	result := make([][3]int, 0, len(list))
	for _, item := range list {
		_, offset := item.Zone()
		result = append(result, [3]int{item.Day(), item.Hour(), offset})
	}
	return result
}

func assertWallTimeAndOffset(
	t *testing.T,
	actual time.Time,
	year int,
	month time.Month,
	day, hour, minute, offset int,
) {
	t.Helper()
	_, actualOffset := actual.Zone()
	if actual.Year() != year ||
		actual.Month() != month ||
		actual.Day() != day ||
		actual.Hour() != hour ||
		actual.Minute() != minute ||
		actualOffset != offset {
		t.Fatalf("unexpected local time: %v (offset %d)", actual, actualOffset)
	}
}
