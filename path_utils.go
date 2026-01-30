package triflestats

import (
	"sort"
	"strconv"
	"strings"
)

// SplitPath splits a dotted path into segments.
func SplitPath(path string) []string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return []string{}
	}
	parts := strings.Split(trimmed, ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

// ResolveConcretePaths expands wildcard paths and map targets into concrete paths.
func ResolveConcretePaths(values []map[string]any, segments []string) [][]string {
	if hasWildcard(segments) {
		return resolvePaths(values, segments)
	}
	if mapTarget(values, segments) {
		expanded := resolvePaths(values, append(append([]string{}, segments...), "*"))
		if len(expanded) == 0 {
			return [][]string{segments}
		}
		return expanded
	}
	return [][]string{segments}
}

func resolvePaths(values []map[string]any, segments []string) [][]string {
	expanded := expandSegments(values, segments, []string{})
	unique := map[string]struct{}{}
	out := make([][]string, 0, len(expanded))
	for _, segs := range expanded {
		key := strings.Join(segs, ".")
		if _, ok := unique[key]; ok {
			continue
		}
		unique[key] = struct{}{}
		out = append(out, segs)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.Join(out[i], ".") < strings.Join(out[j], ".")
	})
	return out
}

func expandSegments(values []map[string]any, segments []string, acc []string) [][]string {
	if len(segments) == 0 {
		return [][]string{acc}
	}

	head := segments[0]
	rest := segments[1:]

	if head == "*" {
		keys := collectKeys(values, acc)
		out := [][]string{}
		for _, key := range keys {
			out = append(out, expandSegments(values, rest, append(acc, key))...)
		}
		return out
	}
	return expandSegments(values, rest, append(acc, head))
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

func hasWildcard(segments []string) bool {
	for _, segment := range segments {
		if segment == "*" {
			return true
		}
	}
	return false
}

func mapTarget(values []map[string]any, segments []string) bool {
	for _, value := range values {
		if node, ok := fetchPath(value, segments).(map[string]any); ok && node != nil {
			return true
		}
	}
	return false
}
