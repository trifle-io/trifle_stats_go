package triflestats

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDriver implements the Driver interface using MongoDB documents.
type MongoDriver struct {
	Collection       *mongo.Collection
	Separator        string
	JoinedIdentifier JoinedIdentifier
	ExpireAfter      time.Duration
	SystemTracking   bool
	BulkWrite        bool
}

// NewMongoDriver creates a MongoDB driver.
func NewMongoDriver(collection *mongo.Collection, joinedIdentifier JoinedIdentifier) *MongoDriver {
	return &MongoDriver{
		Collection:       collection,
		Separator:        "::",
		JoinedIdentifier: joinedIdentifier,
		SystemTracking:   true,
		BulkWrite:        true,
	}
}

// Setup initializes collection indexes for identifier mode and optional TTL.
func (d *MongoDriver) Setup(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.Collection == nil {
		return fmt.Errorf("mongo driver requires Collection")
	}

	indexes := []mongo.IndexModel{}
	switch d.JoinedIdentifier {
	case JoinedFull:
		indexes = append(indexes, mongo.IndexModel{
			Keys:    bson.D{{Key: "key", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
	case JoinedPartial:
		indexes = append(indexes, mongo.IndexModel{
			Keys:    bson.D{{Key: "key", Value: 1}, {Key: "at", Value: -1}},
			Options: options.Index().SetUnique(true),
		})
	case JoinedSeparated:
		indexes = append(indexes, mongo.IndexModel{
			Keys:    bson.D{{Key: "key", Value: 1}, {Key: "granularity", Value: 1}, {Key: "at", Value: -1}},
			Options: options.Index().SetUnique(true),
		})
	default:
		indexes = append(indexes, mongo.IndexModel{
			Keys:    bson.D{{Key: "key", Value: 1}},
			Options: options.Index().SetUnique(true),
		})
	}

	if d.ExpireAfter > 0 {
		indexes = append(indexes, mongo.IndexModel{
			Keys:    bson.D{{Key: "expire_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0),
		})
	}

	_, err := d.Collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (d *MongoDriver) Description() string {
	mode := "J"
	if d.JoinedIdentifier == JoinedPartial {
		mode = "P"
	} else if d.JoinedIdentifier == JoinedSeparated {
		mode = "S"
	}
	return fmt.Sprintf("MongoDriver(%s)", mode)
}

// Inc increments numeric values in-place.
func (d *MongoDriver) Inc(ctx context.Context, keys []Key, values map[string]any) error {
	return d.IncCount(ctx, keys, values, 1)
}

// IncCount increments values and records system tracking count.
func (d *MongoDriver) IncCount(ctx context.Context, keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(ctx, keys, values, "inc", count)
}

// Set writes values without deleting unspecified fields.
func (d *MongoDriver) Set(ctx context.Context, keys []Key, values map[string]any) error {
	return d.SetCount(ctx, keys, values, 1)
}

// SetCount writes values and records system tracking count.
func (d *MongoDriver) SetCount(ctx context.Context, keys []Key, values map[string]any, count int64) error {
	if count <= 0 {
		count = 1
	}
	return d.writeWithOperation(ctx, keys, values, "set", count)
}

// Get fetches values for keys in order using a single Find with $or.
func (d *MongoDriver) Get(ctx context.Context, keys []Key) ([]map[string]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return []map[string]any{}, nil
	}
	if d.Collection == nil {
		return nil, fmt.Errorf("mongo driver requires Collection")
	}

	filters := make([]bson.M, 0, len(keys))
	lookups := make([]string, 0, len(keys))
	for _, key := range keys {
		filter, err := d.identifierFilter(key)
		if err != nil {
			return nil, err
		}
		filters = append(filters, filter)
		lookup, err := d.lookupForKey(key)
		if err != nil {
			return nil, err
		}
		lookups = append(lookups, lookup)
	}

	cursor, err := d.Collection.Find(ctx, bson.M{"$or": filters})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	resultMap := map[string]map[string]any{}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		lookup, err := d.lookupForDocument(doc)
		if err != nil {
			return nil, err
		}
		data, ok := normalizeMongoValue(doc["data"]).(map[string]any)
		if !ok || data == nil {
			data = map[string]any{}
		}
		resultMap[lookup] = data
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	results := make([]map[string]any, 0, len(keys))
	for _, lookup := range lookups {
		data := resultMap[lookup]
		if data == nil {
			data = map[string]any{}
		}
		results = append(results, data)
	}
	return results, nil
}

// Ping stores the latest status payload for the key (separated mode only).
func (d *MongoDriver) Ping(ctx context.Context, key Key, values map[string]any) error {
	if d.JoinedIdentifier != JoinedSeparated {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if d.Collection == nil {
		return fmt.Errorf("mongo driver requires Collection")
	}
	if key.At == nil {
		return fmt.Errorf("ping requires At")
	}

	packed := Pack(map[string]any{"data": values})
	set := bson.M{"at": key.At.UTC()}
	for field, value := range packed {
		set[field] = value
	}
	if expireAt := d.expireAtFor(key.At); expireAt != nil {
		set["expire_at"] = *expireAt
	}

	filter := bson.M{"key": key.Key}
	update := bson.M{"$set": set}
	if d.BulkWrite {
		models := []mongo.WriteModel{
			mongo.NewUpdateManyModel().SetFilter(filter).SetUpdate(update).SetUpsert(true),
		}
		_, err := d.Collection.BulkWrite(ctx, models)
		return err
	}
	_, err := d.Collection.UpdateMany(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}

// Scan returns the latest status for the key (separated mode only).
func (d *MongoDriver) Scan(ctx context.Context, key Key) (time.Time, map[string]any, bool, error) {
	if d.JoinedIdentifier != JoinedSeparated {
		return time.Time{}, nil, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if d.Collection == nil {
		return time.Time{}, nil, false, fmt.Errorf("mongo driver requires Collection")
	}

	opts := options.FindOne().SetSort(bson.D{{Key: "at", Value: -1}})
	var doc bson.M
	err := d.Collection.FindOne(ctx, bson.M{"key": key.Key}, opts).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return time.Time{}, nil, false, nil
	}
	if err != nil {
		return time.Time{}, nil, false, err
	}

	at, ok := mongoTimeValue(doc["at"])
	if !ok {
		return time.Time{}, nil, false, nil
	}
	data, ok := normalizeMongoValue(doc["data"]).(map[string]any)
	if !ok || data == nil {
		data = map[string]any{}
	}
	return at, data, true, nil
}

func (d *MongoDriver) writeWithOperation(ctx context.Context, keys []Key, values map[string]any, op string, count int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return nil
	}
	if d.Collection == nil {
		return fmt.Errorf("mongo driver requires Collection")
	}

	packed := Pack(values)
	if len(packed) == 0 {
		return nil
	}

	models := make([]mongo.WriteModel, 0, len(keys)*2)
	for _, key := range keys {
		filter, err := d.identifierFilter(key)
		if err != nil {
			return err
		}
		update, err := d.buildUpdateDocument(op, packed, key.At)
		if err != nil {
			return err
		}
		models = append(models, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))

		if d.SystemTracking {
			systemKey := Key{
				Key:         systemKeyName,
				Granularity: key.Granularity,
				At:          key.At,
				TrackingKey: key.TrackingKey,
			}
			systemFilter, err := d.identifierFilter(systemKey)
			if err != nil {
				return err
			}
			systemPacked := systemDataFor(key.SystemTrackingKey(), count)
			systemUpdate, err := d.buildUpdateDocument("inc", systemPacked, key.At)
			if err != nil {
				return err
			}
			models = append(models, mongo.NewUpdateOneModel().SetFilter(systemFilter).SetUpdate(systemUpdate).SetUpsert(true))
		}
	}

	if len(models) == 0 {
		return nil
	}
	if d.BulkWrite {
		_, err := d.Collection.BulkWrite(ctx, models)
		return err
	}
	for _, model := range models {
		update, ok := model.(*mongo.UpdateOneModel)
		if !ok {
			continue
		}
		_, err := d.Collection.UpdateOne(ctx, update.Filter, update.Update, options.Update().SetUpsert(true))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *MongoDriver) buildUpdateDocument(op string, packed map[string]any, at *time.Time) (bson.M, error) {
	switch op {
	case "inc":
		inc := bson.M{}
		for key, value := range packed {
			delta, ok := toFloat(value)
			if !ok {
				return nil, fmt.Errorf("increment requires numeric value for key %q", key)
			}
			inc["data."+key] = delta
		}
		update := bson.M{"$inc": inc}
		if expireAt := d.expireAtFor(at); expireAt != nil {
			update["$set"] = bson.M{"expire_at": *expireAt}
		}
		return update, nil
	case "set":
		set := bson.M{}
		for key, value := range packed {
			set["data."+key] = value
		}
		if expireAt := d.expireAtFor(at); expireAt != nil {
			set["expire_at"] = *expireAt
		}
		return bson.M{"$set": set}, nil
	default:
		return nil, fmt.Errorf("invalid operation: %s", op)
	}
}

func (d *MongoDriver) expireAtFor(at *time.Time) *time.Time {
	if d.ExpireAfter <= 0 || at == nil {
		return nil
	}
	expires := at.Add(d.ExpireAfter)
	return &expires
}

func (d *MongoDriver) identifierFilter(k Key) (bson.M, error) {
	switch d.JoinedIdentifier {
	case JoinedFull:
		return bson.M{"key": k.Join(d.Separator)}, nil
	case JoinedPartial:
		if k.At == nil {
			return nil, fmt.Errorf("partial identifier requires At")
		}
		return bson.M{
			"key": k.PartialJoin(d.Separator),
			"at":  *k.At,
		}, nil
	case JoinedSeparated:
		if k.At == nil {
			return nil, fmt.Errorf("separated identifier requires At")
		}
		return bson.M{
			"key":         k.Key,
			"granularity": k.Granularity,
			"at":          *k.At,
		}, nil
	default:
		return bson.M{"key": k.Join(d.Separator)}, nil
	}
}

// lookupForKey builds the map key used to correlate fetched documents with
// requested keys. Timestamps are normalized to UTC unix seconds.
func (d *MongoDriver) lookupForKey(k Key) (string, error) {
	switch d.JoinedIdentifier {
	case JoinedPartial:
		if k.At == nil {
			return "", fmt.Errorf("partial identifier requires At")
		}
		return k.PartialJoin(d.Separator) + "|" + mongoAtLookup(*k.At), nil
	case JoinedSeparated:
		if k.At == nil {
			return "", fmt.Errorf("separated identifier requires At")
		}
		return k.Key + "|" + k.Granularity + "|" + mongoAtLookup(*k.At), nil
	default:
		return k.Join(d.Separator), nil
	}
}

func (d *MongoDriver) lookupForDocument(doc bson.M) (string, error) {
	key, _ := doc["key"].(string)
	switch d.JoinedIdentifier {
	case JoinedPartial:
		at, ok := mongoTimeValue(doc["at"])
		if !ok {
			return "", fmt.Errorf("document missing at value")
		}
		return key + "|" + mongoAtLookup(at), nil
	case JoinedSeparated:
		granularity, _ := doc["granularity"].(string)
		at, ok := mongoTimeValue(doc["at"])
		if !ok {
			return "", fmt.Errorf("document missing at value")
		}
		return key + "|" + granularity + "|" + mongoAtLookup(at), nil
	default:
		return key, nil
	}
}

func mongoAtLookup(t time.Time) string {
	return fmt.Sprintf("%d", t.UTC().Unix())
}

func mongoTimeValue(value any) (time.Time, bool) {
	switch node := value.(type) {
	case time.Time:
		return node.UTC(), true
	case primitive.DateTime:
		return node.Time().UTC(), true
	default:
		return time.Time{}, false
	}
}

func normalizeMongoValue(value any) any {
	switch node := value.(type) {
	case bson.M:
		out := make(map[string]any, len(node))
		for key, value := range node {
			out[key] = normalizeMongoValue(value)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(node))
		for key, value := range node {
			out[key] = normalizeMongoValue(value)
		}
		return out
	case bson.D:
		out := map[string]any{}
		for _, elem := range node {
			out[elem.Key] = normalizeMongoValue(elem.Value)
		}
		return out
	case []any:
		out := make([]any, 0, len(node))
		for _, item := range node {
			out = append(out, normalizeMongoValue(item))
		}
		return out
	default:
		return node
	}
}
