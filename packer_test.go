package triflestats

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestPackingContract(t *testing.T) {
	data, err := os.ReadFile("testdata/stats-paths-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures struct {
		Cases []struct {
			Name                    string
			Input, Logical, Storage map[string]any
		} `json:"packing_cases"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures.Cases {
		t.Run(fixture.Name, func(t *testing.T) {
			for name, got := range map[string]map[string]any{
				"pack": Pack(fixture.Input), "escaped keys": Pack(EscapePathKeys(fixture.Logical)), "tree paths": Pack(PackTree(fixture.Logical)),
			} {
				if !reflect.DeepEqual(got, fixture.Storage) {
					t.Fatalf("%s: %#v != %#v", name, got, fixture.Storage)
				}
			}
			if got := Unpack(fixture.Storage); !reflect.DeepEqual(got, fixture.Logical) {
				t.Fatalf("unpack: %#v", got)
			}
			storedTree := map[string]any{}
			for path, value := range fixture.Storage {
				storedTree = deepMerge(storedTree, buildNested(strings.Split(path, "."), value))
			}
			if got := DecodeTree(storedTree); !reflect.DeepEqual(got, fixture.Logical) {
				t.Fatalf("nested decode: %#v", got)
			}
		})
	}
}

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
