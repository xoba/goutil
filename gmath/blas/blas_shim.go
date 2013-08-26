// +build omitblas

package blas

const PANIC = "blas not implemented in this version!"

func Implemented() bool {
	return false
}

func Dcopy(n int, x []float64, incx int, y []float64, incy int) {
	panic(PANIC)
}

func Dasum(n int, x []float64, incX int) float64 {
	panic(PANIC)
}

func Dnrm2(n int, x []float64, incX int) float64 {
	panic(PANIC)
}

func Dacpy(N int, alpha float64, x []float64, incX int, y []float64, incY int) {
	panic(PANIC)
}

func Ddot(N int, X []float64, incX int, Y []float64, incY int) float64 {
	panic(PANIC)
}
func Sdot(N int, X []float32, incX int, Y []float32, incY int) float32 {
	panic(PANIC)
}
func Sgemm(transA, transB string, M int, N int, K int,
	alpha float32, A []float32, lda int, B []float32, ldb int, beta float32,
	C []float32, ldc int) {
	panic(PANIC)
}
func Dgemm(transA, transB string, M int, N int, K int,
	alpha float64, A []float64, lda int, B []float64, ldb int, beta float64,
	C []float64, ldc int) {
	panic(PANIC)
}

func Dgemv(transA string, M int, N int, alpha float64,
	A []float64, lda int, X []float64, incX int, beta float64,
	Y []float64, incY int) {
	panic(PANIC)
}

func Dsbmv(uplo string, n, k int, alpha float64, a []float64, lda int, x []float64, incx int, beta float64, y []float64, incy int) {
	panic(PANIC)
}
