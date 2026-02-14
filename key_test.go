package triflestats

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestKeyIdentifierModes(t *testing.T) {
	at := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	key := Key{
		Prefix:      "trfl",
		Key:         "event",
		Granularity: "1d",
		At:          &at,
	}

	full := key.Identifier("::", JoinedFull)
	expectedFull := map[string]any{
		"key": "trfl::event::1d::" + strconv.FormatInt(at.Unix(), 10),
	}
	if !reflect.DeepEqual(full, expectedFull) {
		t.Fatalf("full identifier mismatch: %+v", full)
	}

	partial := key.Identifier("::", JoinedPartial)
	expectedPartial := map[string]any{
		"key": "trfl::event::1d",
		"at":  at,
	}
	if !reflect.DeepEqual(partial, expectedPartial) {
		t.Fatalf("partial identifier mismatch: %+v", partial)
	}

	separated := key.Identifier("::", JoinedSeparated)
	expectedSeparated := map[string]any{
		"key":         "event",
		"granularity": "1d",
		"at":          at,
	}
	if !reflect.DeepEqual(separated, expectedSeparated) {
		t.Fatalf("separated identifier mismatch: %+v", separated)
	}
}

func TestParseJoinedIdentifier(t *testing.T) {
	tests := []struct {
		input  string
		expect JoinedIdentifier
	}{
		{input: "full", expect: JoinedFull},
		{input: "FULL", expect: JoinedFull},
		{input: "partial", expect: JoinedPartial},
		{input: "PARTIAL", expect: JoinedPartial},
		{input: "separated", expect: JoinedSeparated},
		{input: "SEPARATED", expect: JoinedSeparated},
		{input: "", expect: JoinedSeparated},
		{input: "unknown", expect: JoinedFull},
	}

	for _, tt := range tests {
		if got := ParseJoinedIdentifier(tt.input); got != tt.expect {
			t.Fatalf("expected %v for input %q, got %v", tt.expect, tt.input, got)
		}
	}
}
