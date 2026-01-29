package triflestats

import (
	"reflect"
	"testing"
)

func TestPackerRoundTrip(t *testing.T) {
	input := map[string]any{
		"count": 1,
		"meta": map[string]any{
			"duration": 2,
			"nested": map[string]any{
				"flag": true,
			},
		},
	}

	packed := Pack(input)
	if packed["count"] == nil || packed["meta.duration"] == nil || packed["meta.nested.flag"] == nil {
		t.Fatalf("packed keys missing: %+v", packed)
	}

	unpacked := Unpack(packed)
	if !reflect.DeepEqual(unpacked, input) {
		t.Fatalf("unpacked mismatch: %+v", unpacked)
	}
}
