package triflestats

import "strings"

// PathCodecVersion identifies the uniform field-name encoding used by all drivers.
// Historical field names are not automatically migrated.
const PathCodecVersion = 1

var pathEncoder = strings.NewReplacer(
	"\x00", "%00", "\"", "%22", "%", "%25", ".", "%2E", "*", "%2A", "$", "%24", "\\", "%5C",
)

var pathDecoder = strings.NewReplacer(
	"%00", "\x00", "%22", "\"", "%25", "%", "%2E", ".", "%2A", "*", "%24", "$", "%5C", "\\",
)

// EncodePathSegment encodes a literal field name, not a whole dotted path.
func EncodePathSegment(segment string) string { return pathEncoder.Replace(segment) }

// DecodePathSegment decodes exactly once, and only the documented sequences.
func DecodePathSegment(segment string) string { return pathDecoder.Replace(segment) }
