package triflestats

import "strings"

// PathSegment retains syntax until selector matching is complete. Concrete map
// keys equal to "*" are literal unless Wildcard is explicitly true.
type PathSegment struct {
	Value         string
	Wildcard      bool
	UnescapedStar bool
}

// ParsePath parses dots and the explicit escapes for dot, star, and backslash.
// Unknown escapes and a trailing backslash are preserved literally.
func ParsePath(path string) []PathSegment {
	if path == "" {
		return []PathSegment{}
	}
	result := []PathSegment{}
	var value strings.Builder
	star := false
	finish := func() {
		text := value.String()
		result = append(result, PathSegment{text, text == "*" && star, star})
		value.Reset()
		star = false
	}
	for index := 0; index < len(path); index++ {
		char := path[index]
		if char == '\\' && index+1 < len(path) && strings.ContainsRune(".*\\", rune(path[index+1])) {
			index++
			value.WriteByte(path[index])
		} else if char == '.' {
			finish()
		} else {
			value.WriteByte(char)
			star = star || char == '*'
		}
	}
	finish()
	return result
}

// ParseLegacyPath leaves historical backslashes unchanged.
func ParseLegacyPath(path string) []PathSegment {
	if path == "" {
		return []PathSegment{}
	}
	parts := strings.Split(path, ".")
	result := make([]PathSegment, 0, len(parts))
	for _, part := range parts {
		result = append(result, PathSegment{part, part == "*", strings.Contains(part, "*")})
	}
	return result
}

// EscapePathSegment escapes a literal key for a public selector.
func EscapePathSegment(value string) string {
	return strings.NewReplacer("\\", "\\\\", ".", "\\.", "*", "\\*").Replace(value)
}

// JoinPath joins literal names, including literal star keys.
func JoinPath(segments []string) string {
	parts := make([]string, len(segments))
	for index, value := range segments {
		parts[index] = EscapePathSegment(value)
	}
	return strings.Join(parts, ".")
}

// RenderPath preserves wildcard tokens when rendering a parsed selector.
func RenderPath(segments []PathSegment) string {
	parts := make([]string, len(segments))
	for index, segment := range segments {
		parts[index] = EscapePathSegment(segment.Value)
		if segment.Wildcard {
			parts[index] = "*"
		}
	}
	return strings.Join(parts, ".")
}

// PathHasWildcard also detects unescaped partial globs, which expressions have
// historically rejected even though formatters expand only whole-star segments.
func PathHasWildcard(path string) bool {
	for _, segment := range ParsePath(path) {
		if segment.UnescapedStar {
			return true
		}
	}
	return false
}

// EscapePathKeys prepares a decoded tree for the path-shaped input API.
func EscapePathKeys(values map[string]any) map[string]any {
	result := make(map[string]any, len(values))
	for key, value := range values {
		if nested, ok := value.(map[string]any); ok {
			value = EscapePathKeys(nested)
		}
		result[EscapePathSegment(key)] = value
	}
	return result
}
