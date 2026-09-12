package triflestats

import (
	"sort"
	"strconv"
)

// SplitPath returns literal decoded segments for a concrete path.
func SplitPath(path string) []string {
	parts := ParsePath(path)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		out = append(out, part.Value)
	}
	return out
}

// ResolveConcretePaths interprets every segment equal to "*" as a wildcard.
// Deprecated: use ResolveSelectorPaths(values, ParsePath(path)) to retain the
// distinction between escaped literal stars and wildcard tokens. Do not pass
// SplitPath(path) here when the selector may contain escaped stars.
func ResolveConcretePaths(values []map[string]any, segments []string) [][]string {
	selector := make([]PathSegment, len(segments))
	for i, segment := range segments {
		selector[i] = PathSegment{Value: segment, Wildcard: segment == "*"}
	}
	return ResolveSelectorPaths(values, selector)
}

// ResolveSelectorPaths expands parsed selectors, preserving escaped literal stars.
func ResolveSelectorPaths(values []map[string]any, segments []PathSegment) [][]string {
	literals := make([]string, len(segments))
	wildcard := false
	for i, segment := range segments {
		literals[i] = segment.Value
		wildcard = wildcard || segment.Wildcard
	}
	if wildcard {
		return resolvePaths(values, segments)
	}
	if mapTarget(values, literals) {
		expanded := resolvePaths(values, append(append([]PathSegment{}, segments...), PathSegment{Value: "*", Wildcard: true}))
		if len(expanded) == 0 {
			return [][]string{literals}
		}
		return expanded
	}
	return [][]string{literals}
}

func resolvePaths(values []map[string]any, segments []PathSegment) [][]string {
	expanded := expandSegments(values, segments, []string{})
	unique := map[string]struct{}{}
	out := make([][]string, 0, len(expanded))
	for _, segs := range expanded {
		key := JoinPath(segs)
		if _, ok := unique[key]; ok {
			continue
		}
		unique[key] = struct{}{}
		out = append(out, segs)
	}
	sort.Slice(out, func(i, j int) bool {
		return JoinPath(out[i]) < JoinPath(out[j])
	})
	return out
}

func expandSegments(values []map[string]any, segments []PathSegment, acc []string) [][]string {
	if len(segments) == 0 {
		return [][]string{acc}
	}

	head := segments[0]
	rest := segments[1:]

	if head.Wildcard {
		keys := collectKeys(values, acc)
		out := [][]string{}
		for _, key := range keys {
			out = append(out, expandSegments(values, rest, append(append([]string{}, acc...), key))...)
		}
		return out
	}
	return expandSegments(values, rest, append(append([]string{}, acc...), head.Value))
}

func collectKeys(values []map[string]any, acc []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		target := fetchPath(value, acc)
		switch node := target.(type) {
		case map[string]any:
			for key := range node {
				seen[key] = struct{}{}
			}
		case []any:
			for idx := range node {
				seen[strconv.Itoa(idx)] = struct{}{}
			}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func fetchPath(data any, segments []string) any {
	current := data
	for _, segment := range segments {
		switch node := current.(type) {
		case map[string]any:
			value, ok := node[segment]
			if !ok {
				return nil
			}
			current = value
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(node) {
				return nil
			}
			current = node[index]
		default:
			return nil
		}
	}
	return current
}

func mapTarget(values []map[string]any, segments []string) bool {
	for _, value := range values {
		if node, ok := fetchPath(value, segments).(map[string]any); ok && node != nil {
			return true
		}
	}
	return false
}
