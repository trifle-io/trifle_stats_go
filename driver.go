package triflestats

// JoinedIdentifier represents identifier mode.
type JoinedIdentifier int

const (
	JoinedFull JoinedIdentifier = iota
	JoinedPartial
	JoinedSeparated
)

// ParseJoinedIdentifier normalizes string/enum into JoinedIdentifier.
func ParseJoinedIdentifier(value string) JoinedIdentifier {
	switch value {
	case "full", "FULL":
		return JoinedFull
	case "partial", "PARTIAL":
		return JoinedPartial
	case "", "separated", "SEPARATED":
		return JoinedSeparated
	default:
		return JoinedFull
	}
}

// Driver is the storage backend interface.
type Driver interface {
	Inc(keys []Key, values map[string]any) error
	Set(keys []Key, values map[string]any) error
	Get(keys []Key) ([]map[string]any, error)
	Description() string
}

// WriteStorage represents the write contract used by Track/Assert.
type WriteStorage interface {
	Inc(keys []Key, values map[string]any) error
	Set(keys []Key, values map[string]any) error
}

// CountDriver extends drivers with operation-count-aware writes, used by Buffer
// to preserve system tracking counts when multiple operations are aggregated.
type CountDriver interface {
	IncCount(keys []Key, values map[string]any, count int64) error
	SetCount(keys []Key, values map[string]any, count int64) error
}
