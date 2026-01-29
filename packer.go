package triflestats

import "strings"

// Pack flattens nested maps into dot-notated keys.
func Pack(input map[string]any) map[string]any {
	return packWithPrefix(input, "")
}

func packWithPrefix(input map[string]any, prefix string) map[string]any {
	out := map[string]any{}
	for k, v := range input {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if val, ok := v.(map[string]any); ok {
			for pk, pv := range packWithPrefix(val, key) {
				out[pk] = pv
			}
			continue
		}
		out[key] = v
	}
	return out
}

// Unpack expands dot-notated keys into nested maps.
func Unpack(input map[string]any) map[string]any {
	out := map[string]any{}
	for key, v := range input {
		if key == "" {
			continue
		}
		parts := strings.Split(key, ".")
		nested := buildNested(parts, v)
		out = deepMerge(out, nested)
	}
	return out
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
