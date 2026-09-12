package triflestats

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestPathContract(t *testing.T) {
	data, err := os.ReadFile("testdata/stats-paths-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Paths []struct {
			Name     string
			Input    string
			Rendered string
			Legacy   []string
			Segments []struct {
				Value         string
				Wildcard      bool
				UnescapedStar bool `json:"unescaped_star"`
			}
		} `json:"path_cases"`
		Codec []struct{ Literal, Encoded string } `json:"codec_cases"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures.Paths {
		t.Run(fixture.Name, func(t *testing.T) {
			parsed := ParsePath(fixture.Input)
			expected := make([]PathSegment, 0, len(fixture.Segments))
			names := make([]string, 0, len(fixture.Segments))
			star := false
			for _, segment := range fixture.Segments {
				expected = append(expected, PathSegment{segment.Value, segment.Wildcard, segment.UnescapedStar})
				names = append(names, segment.Value)
				star = star || segment.UnescapedStar
			}
			if !reflect.DeepEqual(parsed, expected) {
				t.Fatalf("parse: %#v != %#v", parsed, expected)
			}
			if rendered := RenderPath(parsed); rendered != fixture.Rendered {
				t.Fatalf("render: %q != %q", rendered, fixture.Rendered)
			}
			if PathHasWildcard(fixture.Input) != star {
				t.Fatal("incorrect wildcard classification")
			}
			legacy := []string{}
			for _, part := range ParseLegacyPath(fixture.Input) {
				legacy = append(legacy, part.Value)
			}
			if !reflect.DeepEqual(legacy, fixture.Legacy) {
				t.Fatalf("legacy: %#v", legacy)
			}
			roundTrip := []string{}
			for _, part := range ParsePath(JoinPath(names)) {
				roundTrip = append(roundTrip, part.Value)
			}
			if !reflect.DeepEqual(roundTrip, names) {
				t.Fatalf("literal round trip: %#v != %#v", roundTrip, names)
			}
		})
	}
	for _, fixture := range fixtures.Codec {
		t.Run("codec/"+fixture.Literal, func(t *testing.T) {
			if actual := EncodePathSegment(fixture.Literal); actual != fixture.Encoded {
				t.Fatalf("encode: %q != %q", actual, fixture.Encoded)
			}
			if actual := DecodePathSegment(fixture.Encoded); actual != fixture.Literal {
				t.Fatalf("decode: %q != %q", actual, fixture.Literal)
			}
		})
	}
}

func TestConcreteStarPath(t *testing.T) {
	path := JoinPath([]string{"jobs", "*", "count"})
	if path != "jobs.\\*.count" || PathHasWildcard(path) {
		t.Fatalf("literal star became a wildcard: %q", path)
	}
}

func TestEscapePathKeys(t *testing.T) {
	input := map[string]any{"a.b": map[string]any{"*": 3}}
	want := map[string]any{"a\\.b": map[string]any{"\\*": 3}}
	if got := EscapePathKeys(input); !reflect.DeepEqual(got, want) {
		t.Fatalf("escape keys: %#v", got)
	}
}
