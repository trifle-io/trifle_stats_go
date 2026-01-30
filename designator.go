package triflestats

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

// Designator classifies numeric values into bucket labels.
type Designator interface {
	Designate(value any) string
}

// LinearDesignator classifies values using fixed linear steps.
type LinearDesignator struct {
	Min  float64
	Max  float64
	Step int
}

// NewLinearDesignator builds a LinearDesignator.
func NewLinearDesignator(min, max float64, step int) LinearDesignator {
	return LinearDesignator{Min: min, Max: max, Step: step}
}

// Designate returns the bucket label for the value.
func (d LinearDesignator) Designate(value any) string {
	f, ok := toFloat(value)
	if !ok || d.Step == 0 {
		return ""
	}

	if f <= d.Min {
		return formatFloat(f, false)
	}
	if f > d.Max {
		return formatFloat(d.Max, false) + "+"
	}

	ceil := math.Ceil(f)
	step := float64(d.Step)
	bucket := math.Floor(ceil/step) * step
	if math.Mod(ceil, step) != 0 {
		bucket += step
	}
	return formatFloat(bucket, false)
}

// GeometricDesignator classifies values using logarithmic buckets.
type GeometricDesignator struct {
	Min float64
	Max float64
}

// NewGeometricDesignator builds a GeometricDesignator.
func NewGeometricDesignator(min, max float64) GeometricDesignator {
	if min < 0 {
		min = 0
	}
	return GeometricDesignator{Min: min, Max: max}
}

// Designate returns the bucket label for the value.
func (d GeometricDesignator) Designate(value any) string {
	f, ok := toFloat(value)
	if !ok {
		return ""
	}

	if f <= d.Min {
		return formatFloat(d.Min, true)
	}
	if f > d.Max {
		return formatFloat(d.Max, true) + "+"
	}
	if f > 1 {
		power := len(strconv.Itoa(int(math.Floor(f))))
		bucket := math.Pow(10, float64(power))
		return formatFloat(bucket, true)
	}
	if f > 0.1 {
		return formatFloat(1, true)
	}

	zeros := leadingZerosAfterDecimal(f)
	if zeros <= 0 {
		return formatFloat(1, true)
	}
	bucket := 1 / math.Pow(10, float64(zeros))
	return formatFloat(bucket, true)
}

// CustomDesignator classifies values using explicit bucket boundaries.
type CustomDesignator struct {
	Buckets []float64
}

// NewCustomDesignator builds a CustomDesignator with sorted buckets.
func NewCustomDesignator(buckets []float64) CustomDesignator {
	out := append([]float64(nil), buckets...)
	sort.Float64s(out)
	return CustomDesignator{Buckets: out}
}

// Designate returns the bucket label for the value.
func (d CustomDesignator) Designate(value any) string {
	f, ok := toFloat(value)
	if !ok || len(d.Buckets) == 0 {
		return ""
	}

	first := d.Buckets[0]
	last := d.Buckets[len(d.Buckets)-1]

	if f <= first {
		return formatFloat(first, false)
	}
	if f > last {
		return formatFloat(last, false) + "+"
	}

	ceil := math.Ceil(f)
	for _, bucket := range d.Buckets {
		if ceil < bucket {
			return formatFloat(bucket, false)
		}
	}
	return formatFloat(last, false)
}

func formatFloat(value float64, forceDecimal bool) string {
	if forceDecimal {
		if value == math.Trunc(value) {
			return strconv.FormatFloat(value, 'f', 1, 64)
		}
		return strconv.FormatFloat(value, 'f', -1, 64)
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func leadingZerosAfterDecimal(value float64) int {
	value = math.Abs(value)
	s := strconv.FormatFloat(value, 'f', -1, 64)
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return 0
	}
	zeros := 0
	for _, r := range parts[1] {
		if r != '0' {
			break
		}
		zeros++
	}
	return zeros
}
