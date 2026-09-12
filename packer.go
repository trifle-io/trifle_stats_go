package triflestats

import "strings"

// Pack parses public escaped paths and flattens values into storage-safe keys.
func Pack(input map[string]any) map[string]any {
	return packFields(input, nil, false)
}

// PackTree flattens decoded values into public escaped paths, not storage keys.
func PackTree(input map[string]any) map[string]any {
	return packFields(input, nil, true)
}

func packFields(input map[string]any, prefix []string, tree bool) map[string]any {
	out := map[string]any{}
	for k, v := range input {
		segments := []string{k}
		if !tree && k != "" {
			segments = nil
			for _, segment := range ParsePath(k) {
				segments = append(segments, segment.Value)
			}
		}
		path := append(append([]string{}, prefix...), segments...)
		if val, ok := v.(map[string]any); ok {
			for pk, pv := range packFields(val, path, tree) {
				out[pk] = pv
			}
			continue
		}
		key := JoinPath(path)
		if !tree {
			encoded := make([]string, len(path))
			for i, segment := range path {
				encoded[i] = EncodePathSegment(segment)
			}
			key = strings.Join(encoded, ".")
		}
		out[key] = v
	}
	return out
}

// Unpack expands dot-notated keys into nested maps.
func Unpack(input map[string]any) map[string]any {
	out := map[string]any{}
	for key, v := range input {
		parts := strings.Split(key, ".")
		for i, part := range parts {
			parts[i] = DecodePathSegment(part)
		}
		nested := buildNested(parts, decodeTreeValue(v))
		out = deepMerge(out, nested)
	}
	return out
}

// DecodeTree decodes an already nested storage document exactly once.
func DecodeTree(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[DecodePathSegment(key)] = decodeTreeValue(value)
	}
	return out
}

func decodeTreeValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return DecodeTree(typed)
	default:
		return value
	}
}

func buildNested(parts []string, v any) map[string]any {
	if len(parts) == 0 {
		return map[string]any{}
	}
	if len(parts) == 1 {
		return map[string]any{parts[0]: v}
	}
	return map[string]any{parts[0]: buildNested(parts[1:], v)}
}

func deepMerge(a, b map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if existing, ok := out[k]; ok {
			em, ok1 := existing.(map[string]any)
			vm, ok2 := v.(map[string]any)
			if ok1 && ok2 {
				out[k] = deepMerge(em, vm)
				continue
			}
		}
		out[k] = v
	}
	return out
}
