package triflestats

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestMongoDriver_IdentifierFilterModes(t *testing.T) {
	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	key := Key{Key: "events", Granularity: "1h", At: &at}

	full := NewMongoDriver(nil, JoinedFull)
	fullFilter, err := full.identifierFilter(key)
	if err != nil {
		t.Fatalf("full filter failed: %v", err)
	}
	if _, ok := fullFilter["key"]; !ok {
		t.Fatalf("expected full filter to contain key")
	}

	partial := NewMongoDriver(nil, JoinedPartial)
	partialFilter, err := partial.identifierFilter(key)
	if err != nil {
		t.Fatalf("partial filter failed: %v", err)
	}
	if _, ok := partialFilter["at"]; !ok {
		t.Fatalf("expected partial filter to contain at")
	}

	separated := NewMongoDriver(nil, JoinedSeparated)
	separatedFilter, err := separated.identifierFilter(key)
	if err != nil {
		t.Fatalf("separated filter failed: %v", err)
	}
	if _, ok := separatedFilter["granularity"]; !ok {
		t.Fatalf("expected separated filter to contain granularity")
	}
}

func TestMongoDriver_Description(t *testing.T) {
	full := NewMongoDriver(nil, JoinedFull)
	if got := full.Description(); got != "MongoDriver(J)" {
		t.Fatalf("unexpected full description: %s", got)
	}

	partial := NewMongoDriver(nil, JoinedPartial)
	if got := partial.Description(); got != "MongoDriver(P)" {
		t.Fatalf("unexpected partial description: %s", got)
	}

	separated := NewMongoDriver(nil, JoinedSeparated)
	if got := separated.Description(); got != "MongoDriver(S)" {
		t.Fatalf("unexpected separated description: %s", got)
	}
}

func TestMongoDriver_SetupCreatesIndexes(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("setup", func(mt *mtest.T) {
		driver := NewMongoDriver(mt.Coll, JoinedSeparated)
		driver.ExpireAfter = 5 * time.Minute

		mt.AddMockResponses(mtest.CreateSuccessResponse())

		if err := driver.Setup(context.Background()); err != nil {
			mt.Fatalf("setup failed: %v", err)
		}

		event := mt.GetStartedEvent()
		if event == nil {
			mt.Fatalf("expected createIndexes command")
		}
		if event.CommandName != "createIndexes" {
			mt.Fatalf("expected createIndexes command, got %s", event.CommandName)
		}
	})
}

func TestMongoDriver_IdentifierRequiresAtForPartialAndSeparated(t *testing.T) {
	partial := NewMongoDriver(nil, JoinedPartial)
	if _, err := partial.identifierFilter(Key{Key: "events", Granularity: "1h"}); err == nil {
		t.Fatalf("expected missing At error in partial mode")
	}

	separated := NewMongoDriver(nil, JoinedSeparated)
	if _, err := separated.identifierFilter(Key{Key: "events", Granularity: "1h"}); err == nil {
		t.Fatalf("expected missing At error in separated mode")
	}
}

func TestMongoDriver_BuildUpdateDocument(t *testing.T) {
	driver := NewMongoDriver(nil, JoinedFull)
	driver.ExpireAfter = 10 * time.Minute

	at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
	incUpdate, err := driver.buildUpdateDocument("inc", map[string]any{"count": 2, "meta.duration": 3}, &at)
	if err != nil {
		t.Fatalf("build inc update failed: %v", err)
	}
	if _, ok := incUpdate["$inc"]; !ok {
		t.Fatalf("expected $inc clause")
	}
	if _, ok := incUpdate["$set"]; !ok {
		t.Fatalf("expected $set clause for expire_at")
	}

	setUpdate, err := driver.buildUpdateDocument("set", map[string]any{"status": "ok"}, &at)
	if err != nil {
		t.Fatalf("build set update failed: %v", err)
	}
	setClause, ok := setUpdate["$set"].(bson.M)
	if !ok {
		t.Fatalf("expected bson.M set clause")
	}
	if setClause["data.status"] != "ok" {
		t.Fatalf("expected data.status field in set clause, got %+v", setClause)
	}
	if _, ok := setClause["expire_at"]; !ok {
		t.Fatalf("expected expire_at field in set clause")
	}

	if _, err := driver.buildUpdateDocument("inc", map[string]any{"status": "invalid"}, &at); err == nil {
		t.Fatalf("expected non-numeric increment error")
	}
}

func TestMongoDriver_IncCountBulkWriteAndTracking(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("bulk write", func(mt *mtest.T) {
		driver := NewMongoDriver(mt.Coll, JoinedFull)
		driver.BulkWrite = true
		driver.SystemTracking = true

		mt.AddMockResponses(mtest.CreateSuccessResponse())

		at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
		key := Key{
			Key:         "events",
			TrackingKey: "__untracked__",
			Granularity: "1h",
			At:          &at,
		}
		if err := driver.IncCount([]Key{key}, map[string]any{"count": 2}, 3); err != nil {
			mt.Fatalf("inc count failed: %v", err)
		}

		event := mt.GetStartedEvent()
		if event == nil {
			mt.Fatalf("expected started event")
		}
		if event.CommandName != "update" {
			mt.Fatalf("expected update command, got %s", event.CommandName)
		}
		command := event.Command.String()
		if !(strings.Contains(command, "__system__key__") && strings.Contains(command, "__untracked__")) {
			mt.Fatalf("expected system tracking payload in update command: %s", command)
		}
	})
}

func TestMongoDriver_IncCountSequentialUpdatesWhenBulkWriteDisabled(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("sequential update", func(mt *mtest.T) {
		driver := NewMongoDriver(mt.Coll, JoinedFull)
		driver.BulkWrite = false
		driver.SystemTracking = true

		mt.AddMockResponses(
			mtest.CreateSuccessResponse(),
			mtest.CreateSuccessResponse(),
		)

		at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
		key := Key{Key: "events", Granularity: "1h", At: &at}
		if err := driver.Inc([]Key{key}, map[string]any{"count": 1}); err != nil {
			mt.Fatalf("inc failed: %v", err)
		}

		events := mt.GetAllStartedEvents()
		if len(events) != 2 {
			mt.Fatalf("expected 2 update commands, got %d", len(events))
		}
		for _, event := range events {
			if event.CommandName != "update" {
				mt.Fatalf("expected update command, got %s", event.CommandName)
			}
		}
	})
}

func TestMongoDriver_SetCountUsesSetOperation(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("set count", func(mt *mtest.T) {
		driver := NewMongoDriver(mt.Coll, JoinedFull)
		driver.BulkWrite = true
		driver.SystemTracking = true

		mt.AddMockResponses(mtest.CreateSuccessResponse())

		at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
		key := Key{Key: "events", Granularity: "1h", At: &at}
		if err := driver.SetCount([]Key{key}, map[string]any{"status": "ok"}, 2); err != nil {
			mt.Fatalf("set count failed: %v", err)
		}

		event := mt.GetStartedEvent()
		if event == nil {
			mt.Fatalf("expected started event")
		}
		command := event.Command.String()
		if !strings.Contains(command, "$set") {
			mt.Fatalf("expected set update payload, got %s", command)
		}
	})
}

func TestMongoDriver_GetReturnsValuesInOrder(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("get order", func(mt *mtest.T) {
		driver := NewMongoDriver(mt.Coll, JoinedFull)
		driver.SystemTracking = false

		at := time.Date(2025, 2, 1, 11, 0, 0, 0, time.UTC)
		key1 := Key{Key: "events", Granularity: "1h", At: &at}
		key2 := Key{Key: "events", Granularity: "1h", At: ptrTime(at.Add(time.Hour))}
		key1Joined := key1.Join("::")

		ns := mt.DB.Name() + "." + mt.Coll.Name()
		mt.AddMockResponses(
			mtest.CreateCursorResponse(0, ns, mtest.FirstBatch,
				bson.D{
					{Key: "key", Value: key1Joined},
					{Key: "data", Value: bson.D{
						{Key: "count", Value: int32(5)},
						{Key: "meta", Value: bson.D{{Key: "duration", Value: int32(2)}}},
					}},
				},
			),
			mtest.CreateCursorResponse(0, ns, mtest.FirstBatch),
		)

		values, err := driver.Get([]Key{key1, key2})
		if err != nil {
			mt.Fatalf("get failed: %v", err)
		}
		if len(values) != 2 {
			mt.Fatalf("expected two rows, got %d", len(values))
		}
		if got, ok := toFloat(values[0]["count"]); !ok || got != 5 {
			mt.Fatalf("expected first row count 5, got %#v", values[0]["count"])
		}
		meta := values[0]["meta"].(map[string]any)
		if got, ok := toFloat(meta["duration"]); !ok || got != 2 {
			mt.Fatalf("expected first row meta.duration 2, got %#v", meta["duration"])
		}
		if len(values[1]) != 0 {
			mt.Fatalf("expected second row to be empty, got %+v", values[1])
		}
	})
}

func TestNormalizeMongoValue(t *testing.T) {
	value := bson.M{
		"meta": bson.D{{Key: "duration", Value: int32(2)}},
		"arr":  []any{bson.M{"count": int32(1)}},
	}
	normalized := normalizeMongoValue(value).(map[string]any)

	meta := normalized["meta"].(map[string]any)
	if meta["duration"] != int32(2) {
		t.Fatalf("unexpected normalized duration: %#v", meta["duration"])
	}
	arr := normalized["arr"].([]any)
	first := arr[0].(map[string]any)
	if first["count"] != int32(1) {
		t.Fatalf("unexpected normalized array value: %#v", first["count"])
	}
}
