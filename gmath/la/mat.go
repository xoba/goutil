// linear algebra routines.
package la

import (
	"fmt"
	"github.com/xoba/goutil/gmath/blas"
	"strings"
)

// vector
type Vector struct {
	Size     int
	Stride   int
	Elements []float64
}

func NewVector(m int) *Vector {
	return &Vector{Size: m, Stride: 1, Elements: make([]float64, m)}
}

func (m *Vector) Set(i int, v float64) {
	m.Elements[i*m.Stride] = v
}
func (m *Vector) Get(i int) float64 {
	return m.Elements[i*m.Stride]
}

func (m *Vector) AsColumnVector() *Matrix {
	return &Matrix{
		Rows:         m.Size,
		Cols:         1,
		ColumnStride: m.Size,
		Elements:     m.Elements,
	}

}

// a column-major matrix
type Matrix struct {
	Rows, Cols   int
	ColumnStride int // increment index by this, to move to next column in same row
	Elements     []float64
}

func NewMatrix(m, n int) *Matrix {
	return &Matrix{Rows: m, Cols: n, ColumnStride: m, Elements: make([]float64, m*n)}
}
func NewMatrixWithElements(m, n int, elements []float64) *Matrix {
	return &Matrix{Rows: m, Cols: n, ColumnStride: m, Elements: elements}
}

func (m *Matrix) Copy() *Matrix {
	c := make([]float64, len(m.Elements))
	if n := copy(c, m.Elements); n != len(m.Elements) {
		panic("can't copy")
	}
	return NewMatrixWithElements(m.Rows, m.Cols, c)
}

func (m *Matrix) index(i, j int) int {
	return j*m.ColumnStride + i
}

func (m *Matrix) Set(i, j int, v float64) {
	m.Elements[m.index(i, j)] = v
}
func (m *Matrix) Inc(i, j int, v float64) {
	m.Elements[m.index(i, j)] += v
}
func (m *Matrix) Get(i, j int) float64 {
	return m.Elements[m.index(i, j)]
}

func (m *Matrix) String() string {
	var out []string
	out = append(out, fmt.Sprintf("%d x %d float64 matrix", m.Rows, m.Cols))
	if m.Rows*m.Cols < 1000 {
		out = append(out, ":")
		out = append(out, "\n")
		out = append(out, "\t[")
		for i := 0; i < m.Rows; i++ {
			if i > 0 {
				out = append(out, " ")
			}
			out = append(out, "[")
			var row []string
			for j := 0; j < m.Cols; j++ {
				v := m.Elements[m.index(i, j)]
				row = append(row, fmt.Sprintf("%9.2e", v))
			}
			out = append(out, strings.Join(row, ", "))
			out = append(out, "]")
			if i < m.Rows-1 {
				out = append(out, "\n\t")
			}
		}
		out = append(out, "]")
	}
	return strings.Join(out, "")
}

func Multiply(m ...*Matrix) *Matrix {
	switch len(m) {
	case 0:
		return nil
	case 1:
		return m[0]
	default:
		var left = m[0]
		for i := 1; i < len(m); i++ {
			left = MultiplyPair(left, m[i])
		}
		return left
	}
}

func MultiplyPair(a, b *Matrix) *Matrix {
	c := NewMatrix(a.Rows, b.Cols)
	blas.Dgemm("N", "N", a.Rows, b.Cols, a.Cols, 1.0, a.Elements, a.ColumnStride, b.Elements, b.ColumnStride, 0.0, c.Elements, c.ColumnStride)
	return c
}
