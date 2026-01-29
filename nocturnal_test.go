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
