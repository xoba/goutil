package la

import (
	"github.com/xoba/goutil/gmath/blas"
	"math"
	"math/rand"
	"testing"
)

func TestDGemv1(t *testing.T) {

	a := New(2, 2)
	a.Elements = []float64{1, 3, 2, 4}

	x := []float64{4, 5}
	y := []float64{10, 11}

	alpha := 1.0
	beta := 3.0

	z0 := []float64{0, 0}

	for i := 0; i < 2; i++ {
		z0[i] += beta * y[i]
		for j := 0; j < 2; j++ {
			z0[i] += alpha * a.Get(i, j) * x[j]
		}
	}

	blas.Dgemv("N", 2, 2, alpha, a.Elements, 2, x, 1, beta, y, 1)

	for i := 0; i < len(z0); i++ {
		if z0[i] != y[i] {
			t.Errorf("%d. %f vs %f\n", i, z0[i], y[i])
		}
	}
}

func TestDGemv2(t *testing.T) {
	ri := func() int {
		return 10 + rand.Intn(5)
	}
	for i := 0; i < 100; i++ {
		m, n := ri(), ri()
		r := func() float64 {
			return rand.NormFloat64()
		}
		a := New(m, n)
		x := New(1, n)
		y := New(1, m)
		z := New(1, m)
		for i := 0; i < m; i++ {
			y.Set(0, i, r())
		}
		for j := 0; j < n; j++ {
			x.Set(0, j, r())
			for i := 0; i < m; i++ {
				a.Set(i, j, r())
			}
		}
		alpha := r()
		beta := r()
		for i := 0; i < m; i++ {
			z.Set(0, i, beta*y.Get(0, i))
			for j := 0; j < n; j++ {
				z.Set(0, i, z.Get(0, i)+alpha*a.Get(i, j)*x.Get(0, j))
			}
		}
		blas.Dgemv("N", a.Rows, a.Cols, alpha, a.Elements, a.ColumnStride, x.Elements, x.ColumnStride, beta, y.Elements, y.ColumnStride)
		for i := 0; i < len(z.Elements); i++ {
			if a, b := z.Elements[i], y.Elements[i]; math.Abs(a-b) > eps {
				t.Errorf("%d. %f vs %f\n", i, a, b)
			}
		}
	}
}

const eps = 0.00000000000001
