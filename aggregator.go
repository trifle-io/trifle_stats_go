package triflestats

// AggregateSum calculates summed values for a path.
func (s Series) AggregateSum(path string, slices int) []any {
	return aggregateSlices(s.collectPathValues(path), slices, sumSlice)
}

// AggregateMean calculates mean values for a path.
func (s Series) AggregateMean(path string, slices int) []any {
	return aggregateSlices(s.collectPathValues(path), slices, meanSlice)
}

// AggregateMin calculates min values for a path.
func (s Series) AggregateMin(path string, slices int) []any {
	return aggregateSlices(s.collectPathValues(path), slices, minSlice)
}

// AggregateMax calculates max values for a path.
func (s Series) AggregateMax(path string, slices int) []any {
	return aggregateSlices(s.collectPathValues(path), slices, maxSlice)
}

func (s Series) collectPathValues(path string) []any {
	segments := SplitPath(path)
	out := make([]any, 0, len(s.Values))
	for _, row := range s.Values {
		out = append(out, fetchPath(row, segments))
	}
	return out
}

type sliceAggregator func([]any) any

func aggregateSlices(values []any, slices int, aggregator sliceAggregator) []any {
	groups := sliceValues(values, slices)
	results := make([]any, 0, len(groups))
	for _, group := range groups {
		results = append(results, aggregator(group))
	}
	return results
}

func sliceValues(values []any, slices int) [][]any {
	count := len(values)
	if count == 0 {
		return [][]any{}
	}
	if slices <= 1 {
		return [][]any{values}
	}
	sliceSize := count / slices
	if sliceSize <= 0 {
		return [][]any{values}
	}
	start := count - (sliceSize * slices)
	relevant := values[start:]
	out := make([][]any, 0, slices)
	for i := 0; i+sliceSize <= len(relevant); i += sliceSize {
		out = append(out, relevant[i:i+sliceSize])
	}
	return out
}

func sumSlice(values []any) any {
	sum := 0.0
	for _, v := range values {
		if f, ok := toFloat(v); ok {
			sum += f
		}
	}
	return sum
}

func meanSlice(values []any) any {
	sum := 0.0
	count := 0
	for _, v := range values {
		if f, ok := toFloat(v); ok {
			sum += f
			count++
		}
	}
	if count == 0 {
		return float64(0)
	}
	return sum / float64(count)
}

func minSlice(values []any) any {
	var min float64
	has := false
	for _, v := range values {
		if f, ok := toFloat(v); ok {
			if !has || f < min {
				min = f
				has = true
			}
		}
	}
	if !has {
		return nil
	}
	return min
}

func maxSlice(values []any) any {
	var max float64
	has := false
	for _, v := range values {
		if f, ok := toFloat(v); ok {
			if !has || f > max {
				max = f
				has = true
			}
		}
	}
	if !has {
		return nil
	}
	return max
}
