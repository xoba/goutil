package la

import (
	"fmt"

	"github.com/xoba/goutil/gmath/lapack"
)

type Svd struct {
	U, S, Vt *Matrix
}

func (s *Svd) String() string {
	return fmt.Sprintf("U: %d x %d; S: %d x %d; Vt: %d x %d\n",
		s.U.Rows, s.U.Cols,
		s.S.Rows, s.S.Cols,
		s.Vt.Rows, s.Vt.Cols,
	)
}

func ComputeSvd(a *Matrix) *Svd {

	rank := min(a.Rows, a.Cols)

	u := NewMatrix(a.Rows, rank)
	s := NewMatrix(rank, 1)
	vt := NewMatrix(rank, a.Cols)

	info := lapack.Dgesdd("S", a.Rows, a.Cols, a.Elements, a.ColumnStride,
		s.Elements,
		u.Elements, u.ColumnStride,
		vt.Elements, vt.ColumnStride)

	if info != 0 {
		panic("svd unsuccessful")
	}

	return &Svd{U: u, S: NewDiagonalMatrixWithElements(s.Elements), Vt: vt}
}

func min(i, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}

func NewDiagonalMatrixWithElements(e []float64) *Matrix {
	m := len(e)
	out := NewMatrix(m, m)
	for i := 0; i < m; i++ {
		out.Set(i, i, e[i])
	}
	return out
}
