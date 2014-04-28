package stats

import (
	"math"
	"math/rand"
	"testing"
)

func TestMean(t *testing.T) {
	equal("mean", t, 2, Mean([]float64{1, 2, 3}))
}

func TestMean1(t *testing.T) {
	a := []float64{}
	v := Mean(a)
	if !math.IsNaN(v) {
		t.Errorf("empty mean is not NaN: %f\n", v)
	}
}

func TestSd1(t *testing.T) {
	a := []float64{}
	v := Sd(a)
	if !math.IsNaN(v) {
		t.Errorf("empty sd is not NaN: %f\n", v)
	}
}

func TestSd2(t *testing.T) {
	a := []float64{1}
	v := Sd(a)
	if !math.IsNaN(v) {
		t.Errorf("single element sd is not NaN: %f\n", v)
	}
}

func TestSd(t *testing.T) {
	equal("sd", t, 1, Sd([]float64{1, 2, 3}))
}

func TestMax(t *testing.T) {
	equal("max", t, 10, Max([]float64{1, 2, 10, 3}))
}

func TestMin(t *testing.T) {
	equal("max", t, -10, Min([]float64{1, 2, -10, 3}))
}

func TestMedian0(t *testing.T) {
	a := []float64{}
	v := Median(true, a)
	if !math.IsNaN(v) {
		t.Errorf("empty median is not NaN: %f\n", v)
	}
}

func TestPercentile1(t *testing.T) {
	equal("percentile1", t, 2, Percentile(true, []float64{1, 2, 3}, 0.50))
}
func TestPercentile2(t *testing.T) {
	equal("percentile2", t, 1, Percentile(true, []float64{1, 2, 3}, 0))
}
func TestPercentile3(t *testing.T) {
	equal("percentile3", t, 3, Percentile(true, []float64{1, 2, 3}, 1))
}
func TestPercentile4(t *testing.T) {
	equal("percentile4", t, 2.5, Percentile(true, []float64{1, 2, 3, 4}, 0.50))
}

func TestPercentile5(t *testing.T) {
	list := []float64{34, 2, 25, 6, 76, 7, 4, 3, 3, 343433, 3.4354}
	last := -math.MaxFloat64
	for i := 0; i <= 100; i++ {
		p := Percentile(true, list, 0.50)
		if p < last {
			t.Errorf("decreasing percentile value: %f --> %f", last, p)
		}
		last = p
	}
}

func TestEffectiveNumber(t *testing.T) {
	a := []float64{0.2, 0.2, 0.2, 0.2, 0.2}
	v := EffectiveNumber(a)
	equal("effective number", t, 5, v)
}
func TestEffectiveNumber2(t *testing.T) {
	a := []float64{0.3, 0.3, 0.3, 0.3, 0.3}
	v := EffectiveNumber(a)
	equal("effective number", t, 5, v)
}
func TestEffectiveNumber3(t *testing.T) {
	a := []float64{1, 1, 1}
	v := EffectiveNumber(a)
	equal("effective number", t, 3, v)
}
func TestEffectiveNumber3a(t *testing.T) {
	a := []float64{1, 1, 1, 0, 0, 0}
	v := EffectiveNumber(a)
	equal("effective number", t, 3, v)
}

func TestMedian(t *testing.T) {
	equal("median", t, 2, Median(true, shuffle([]float64{1, 2, 3})))
}

func TestMedian2(t *testing.T) {
	equal("median", t, 1, Median(true, shuffle([]float64{1})))
}

func TestMedian3(t *testing.T) {
	equal("median", t, 1.5, Median(true, shuffle([]float64{1, 2})))
}

func TestMedian4(t *testing.T) {
	equal("median", t, 4, Median(true, shuffle([]float64{1, 2, 3, 4, 5, 6, 7})))
}

func TestMedian5(t *testing.T) {
	equal("median", t, 6, Median(true, shuffle([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})))
}

func TestMedian6(t *testing.T) {
	equal("median", t, 6.5, Median(true, shuffle([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})))
}

func TestMedianX(t *testing.T) {
	equal("median", t, 2, Median(false, shuffle([]float64{1, 2, 3})))
}

func TestMedianX2(t *testing.T) {
	equal("median", t, 1, Median(false, shuffle([]float64{1})))
}

func TestMedianX3(t *testing.T) {
	equal("median", t, 1.5, Median(false, shuffle([]float64{1, 2})))
}

func TestMedianX4(t *testing.T) {
	equal("median", t, 4, Median(false, shuffle([]float64{1, 2, 3, 4, 5, 6, 7})))
}

func TestMedianX5(t *testing.T) {
	equal("median", t, 6, Median(false, shuffle([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})))
}

func TestMedianX6(t *testing.T) {
	equal("median", t, 6.5, Median(false, shuffle([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})))
}

func shuffle(list []float64) (out []float64) {
	for _, x := range rand.Perm(len(list)) {
		out = append(out, list[x])
	}
	return
}

const eps = 0.00000000000001

func equal(name string, t *testing.T, correct, computed float64) {
	if math.Abs(correct-computed) > eps {
		t.Errorf("bad %s, off by %.20f", name, computed-correct)
	}
}
