package triflestats

import "math"

// TransformAdd adds two paths and stores the result.
func (s Series) TransformAdd(left, right, response string) Series {
	if response == "" {
		response = "add"
	}
	return s.transformBinary(left, right, response, func(a, b float64) (float64, bool) {
		return a + b, true
	})
}

// TransformSubtract subtracts right from left and stores the result.
func (s Series) TransformSubtract(left, right, response string) Series {
	if response == "" {
		response = "subtract"
	}
	return s.transformBinary(left, right, response, func(a, b float64) (float64, bool) {
		return a - b, true
	})
}

// TransformMultiply multiplies two paths and stores the result.
func (s Series) TransformMultiply(left, right, response string) Series {
	if response == "" {
		response = "multiply"
	}
	return s.transformBinary(left, right, response, func(a, b float64) (float64, bool) {
		return a * b, true
	})
}

// TransformDivide divides left by right and stores the result.
func (s Series) TransformDivide(left, right, response string) Series {
	if response == "" {
		response = "divide"
	}
	return s.transformBinary(left, right, response, func(a, b float64) (float64, bool) {
		if b == 0 {
			return 0, true
		}
		return a / b, true
	})
}

// TransformRatio divides left by right, multiplies by 100, and stores the result.
func (s Series) TransformRatio(left, right, response string) Series {
	if response == "" {
		response = "ratio"
	}
	return s.transformBinary(left, right, response, func(a, b float64) (float64, bool) {
		if b == 0 {
			return 0, true
		}
		return (a / b) * 100, true
	})
}

// TransformSum sums multiple paths and stores the result.
func (s Series) TransformSum(paths []string, response string) Series {
	if response == "" {
		response = "sum"
	}
	return s.transformMulti(paths, response, func(values []float64) (float64, bool) {
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum, true
	})
}

// TransformMin stores the minimum of multiple paths.
func (s Series) TransformMin(paths []string, response string) Series {
	if response == "" {
		response = "min"
	}
	return s.transformMulti(paths, response, func(values []float64) (float64, bool) {
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min, true
	})
}

// TransformMax stores the maximum of multiple paths.
func (s Series) TransformMax(paths []string, response string) Series {
	if response == "" {
		response = "max"
	}
	return s.transformMulti(paths, response, func(values []float64) (float64, bool) {
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max, true
	})
}

// TransformMean stores the mean of multiple paths.
func (s Series) TransformMean(paths []string, response string) Series {
	if response == "" {
		response = "mean"
	}
	return s.transformMulti(paths, response, func(values []float64) (float64, bool) {
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values)), true
	})
}

// TransformStandardDeviation calculates standard deviation using sum, count, and square paths.
func (s Series) TransformStandardDeviation(sumPath, countPath, squarePath, response string) Series {
	if response == "" {
		response = "sd"
	}
	responseSegments := SplitPath(response)
	sumSegments := SplitPath(sumPath)
	countSegments := SplitPath(countPath)
	squareSegments := SplitPath(squarePath)

	values := make([]map[string]any, 0, len(s.Values))
	for _, row := range s.Values {
		sumValue, ok1 := toFloat(fetchPath(row, sumSegments))
		countValue, ok2 := toFloat(fetchPath(row, countSegments))
		squareValue, ok3 := toFloat(fetchPath(row, squareSegments))
		if !ok1 || !ok2 || !ok3 {
			values = append(values, row)
			continue
		}
		denominator := countValue * (countValue - 1)
		if denominator <= 0 {
			values = append(values, row)
			continue
		}
		numerator := (countValue * squareValue) - (sumValue * sumValue)
		result := numerator / denominator
		if result < 0 {
			result = 0
		}
		sd := math.Sqrt(result)
		if math.IsNaN(sd) || math.IsInf(sd, 0) {
			sd = 0
		}
		values = append(values, mergeValue(row, responseSegments, sd))
	}
	return Series{At: s.At, Values: values}
}

func (s Series) transformBinary(left, right, response string, op func(float64, float64) (float64, bool)) Series {
	if response == "" {
		response = "result"
	}
	leftSegments := SplitPath(left)
	rightSegments := SplitPath(right)
	responseSegments := SplitPath(response)

	values := make([]map[string]any, 0, len(s.Values))
	for _, row := range s.Values {
		leftValue, ok1 := toFloat(fetchPath(row, leftSegments))
		rightValue, ok2 := toFloat(fetchPath(row, rightSegments))
		if !ok1 || !ok2 {
			values = append(values, row)
			continue
		}
		result, ok := op(leftValue, rightValue)
		if !ok {
			values = append(values, row)
			continue
		}
		values = append(values, mergeValue(row, responseSegments, result))
	}
	return Series{At: s.At, Values: values}
}

func (s Series) transformMulti(paths []string, response string, op func([]float64) (float64, bool)) Series {
	if response == "" {
		response = "result"
	}
	if len(paths) == 0 {
		return s
	}
	segments := make([][]string, 0, len(paths))
	for _, path := range paths {
		segments = append(segments, SplitPath(path))
	}
	responseSegments := SplitPath(response)

	values := make([]map[string]any, 0, len(s.Values))
	for _, row := range s.Values {
		numeric := make([]float64, 0, len(segments))
		skip := false
		for _, segs := range segments {
			value, ok := toFloat(fetchPath(row, segs))
			if !ok {
				skip = true
				break
			}
			numeric = append(numeric, value)
		}
		if skip || len(numeric) == 0 {
			values = append(values, row)
			continue
		}
		result, ok := op(numeric)
		if !ok {
			values = append(values, row)
			continue
		}
		values = append(values, mergeValue(row, responseSegments, result))
	}
	return Series{At: s.At, Values: values}
}

func mergeValue(row map[string]any, responseSegments []string, value any) map[string]any {
	if len(responseSegments) == 0 {
		return row
	}
	patch := Unpack(map[string]any{joinSegments(responseSegments): value})
	return deepMerge(row, patch)
}
