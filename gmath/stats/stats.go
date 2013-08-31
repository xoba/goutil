// various statistical routines
package stats

import (
	"fmt"
	"math"
	"sort"
)

func MeanVariance(list []float64) (mean float64, variance float64) {
	n := float64(len(list))
	switch n {
	case 0:
		return math.NaN(), math.NaN()
	case 1:
		return list[0], math.NaN()
	}
	var sum1, sum2 float64
	for _, x := range list {
		sum1 += x
		sum2 += x * x
	}
	mean = sum1 / n
	variance = n / (n - 1) * (sum2/n - mean*mean)
	return
}

func Mean(list []float64) float64 {
	n := float64(len(list))
	switch n {
	case 0:
		return math.NaN()
	case 1:
		return list[0]
	}
	var sum1 float64
	for _, x := range list {
		sum1 += x
	}
	return sum1 / n
}

func Variance(list []float64) float64 {
	_, v := MeanVariance(list)
	return v
}

func Sd(list []float64) float64 {
	return math.Sqrt(Variance(list))
}

func RemoveNaNsAndInfs(list []float64) (cleaned []float64) {
	for _, x := range list {
		if !(math.IsNaN(x) || math.IsInf(x, 0)) {
			cleaned = append(cleaned, x)
		}
	}
	return
}

func Abs(list []float64) (abs []float64) {
	for _, x := range list {
		abs = append(abs, math.Abs(x))
	}
	return
}

func Sum(list []float64) float64 {
	var out float64
	for _, x := range list {
		out += x
	}
	return out
}

func SquareSum(list []float64) float64 {
	var out float64
	for _, x := range list {
		out += x + x
	}
	return out
}

func AbsSum(list []float64) float64 {
	var out float64
	for _, x := range list {
		out += math.Abs(x)
	}
	return out
}

func Min(list []float64) float64 {
	min := math.MaxFloat64
	for _, x := range list {
		if x < min {
			min = x
		}
	}
	return min
}

func Max(list []float64) float64 {
	max := -math.MaxFloat64
	for _, x := range list {
		if x > max {
			max = x
		}
	}
	return max
}

func EffectiveNumber(weights []float64) float64 {
	var m1, m2 float64
	for _, w := range weights {
		m1 += math.Abs(w)
		m2 += w * w
	}
	return m1 * m1 / m2
}

// median by sort, boolean controls whether sort is done in place or on a copy
func Median(inPlace bool, list []float64) float64 {
	return Percentile(inPlace, list, 50)
}

type Tool struct {
}

func (t *Tool) Tags() []string {
	return []string{}
}
func (m *Tool) Name() string {
	return "stats"
}
func (m *Tool) Description() string {
	return "debug stats"
}
func (*Tool) Run(args []string) {
	for i, x := range GetLogRange(-100, -1, 21) {
		fmt.Printf("%d: %f (%f)\n", i, x, 10*math.Log10(x))
	}
}

// continuous non-decreasing output vs fraction input
func Percentile(inPlace bool, list []float64, fraction float64) float64 {

	l := len(list)
	n := float64(l)

	switch n {
	case 0:
		return math.NaN()
	case 1:
		return list[0]
	}

	if !inPlace {
		c := make([]float64, len(list))
		copy(c, list)
		list = c
	}

	sort.Float64s(list)

	p := fraction * (n - 1)

	switch {
	case fraction < 0 || fraction > 1:
		return math.NaN()
	case fraction == 0:
		return list[0]
	case fraction == 100:
		return list[l-1]
	case int(p) == l-1:
		return list[l-1]
	}

	leftI := int(p)
	rightI := int(p) + 1

	leftV := list[leftI]
	rightV := list[rightI]

	leftF := float64(leftI) / (n - 1)
	rightF := float64(rightI) / (n - 1)

	f := (fraction - leftF) / (rightF - leftF)

	return leftV + f*(rightV-leftV)
}

func GetLogRange(min, max float64, n int) (out []float64) {

	switch {
	case max < min || min < 0 || max < 0:
		return []float64{}
	case min == max:
		return []float64{min}
	}

	min = math.Log(min)
	max = math.Log(max)
	dx := (max - min) / (float64(n) - 1)

	for i := 0; i < n; i++ {
		log := min + float64(i)*dx
		out = append(out, math.Exp(log))
	}
	return
}

func Negate(list []float64) (out []float64) {
	for _, x := range list {
		out = append(out, -x)
	}
	return
}
