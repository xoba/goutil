// +build !omitblas

package blas

import (
	"math"
	"math/rand"
	"testing"
)

type Vec struct {
	V []float64 // values
	S int       // stride
}

func NewVec(v []float64) *Vec {
	return &Vec{v, 1}
}
func (v *Vec) WithStride(s int) *Vec {
	return &Vec{newStride(v.V, v.S, s), s}
}
func (v *Vec) Len() int {
	return len(v.V) / v.S
}
func (v *Vec) Get(i int) float64 {
	return v.V[i*v.S]
}

func TestDsbmv(t *testing.T) {
	alpha := 1.0
	beta := 0.0
	n := 3
	k := 0

	a := NewVec([]float64{1, 2, 3})
	x := NewVec([]float64{4, 5, 6})
	y := NewVec([]float64{0, 0, 0})

	Dsbmv("L", n, k, alpha, a.V, 1, x.V, 1, beta, y.V, 1)

	z := NewVec([]float64{4, 10, 18})

	equalV("Dsbmv", z, y, t)

}

func TestDcopy(t *testing.T) {
	n := 100
	var x, y []float64
	for i := 0; i < n; i++ {
		x = append(x, rand.Float64())
		y = append(y, 0)
	}
	xv := NewVec(x)
	yv := NewVec(y)
	Dcopy(n, x, 1, y, 1)
	equalV("copy", xv, yv, t)
}

func TestDdot(t *testing.T) {
	x := NewVec([]float64{1, 2, 3}).WithStride(5)
	y := NewVec([]float64{4, 5, 6}).WithStride(5)
	var sum float64
	{
		for i := 0; i < x.Len(); i++ {
			sum += x.Get(i) * y.Get(i)
		}
	}
	r := Ddot(x.Len(), x.V, x.S, y.V, y.S)
	equalD(sum, r, t)
}

func TestDasum(t *testing.T) {
	incx := 5
	x := NewVec([]float64{1, 2, 3})
	var sum float64
	{
		for i := 0; i < x.Len(); i++ {
			sum += math.Abs(x.Get(i))
		}
	}
	x = x.WithStride(incx)
	r := Dasum(3, x.V, x.S)
	equalD(sum, r, t)
}

func TestDnrm2(t *testing.T) {
	incx := 5
	x := NewVec([]float64{1, 2, 3})
	var norm float64
	{
		for i := 0; i < x.Len(); i++ {
			norm += math.Pow(x.Get(i), 2)
		}
		norm = math.Sqrt(norm)
	}
	x = x.WithStride(incx)
	r := Dnrm2(3, x.V, x.S)
	equalD(norm, r, t)
}

const eps = 0.00000000000001

func TestAxpy(t *testing.T) {

	incx := 5
	incy := 2

	x := NewVec([]float64{1, 2, 3})
	y := NewVec([]float64{10, 11, 12})
	z := NewVec([]float64{12, 15, 18})

	x = x.WithStride(incx)
	y = y.WithStride(incy)

	Dacpy(3, 2, x.V, x.S, y.V, y.S)
	equalV("axpy", z, y, t)
}

func newStride(x []float64, from, to int) (out []float64) {
	if from == to {
		return x
	}
	for i := 0; i < len(x); i += from {
		out = append(out, x[i])
		for j := 0; j < to-1; j++ {
			out = append(out, 0)
		}
	}
	return
}
func equalD(expected, computed float64, t *testing.T) {
	if math.Abs(expected-computed) > eps {
		t.Errorf("excess of %f\n", computed-expected)
	}
}

func equalV(name string, x, y *Vec, t *testing.T) {
	if x.Len() != y.Len() {
		t.Errorf("%s: len(x) = %d vs len(y) = %d\n", name, x.Len(), y.Len())
		return
	}
	for i := 0; i < x.Len(); i++ {
		if x.Get(i) != y.Get(i) {
			t.Errorf("%s: expected[%d] = %f vs computed[%d] = %f\n", name, i, x.Get(i), i, y.Get(i))
		}
	}
}
