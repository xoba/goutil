// +build omitblas

package lapack

const PANIC = "lapack not implemented in this version!"

func Sgesdd(jobz string, M, N int, A []float32, lda int, S []float32, U []float32,
	ldu int, Vt []float32, ldvt int) int {
	panic(PANIC)
}

func Dgesdd(jobz string, M, N int, A []float64, lda int, S []float64, U []float64,
	ldu int, Vt []float64, ldvt int) int {
	panic(PANIC)
}
