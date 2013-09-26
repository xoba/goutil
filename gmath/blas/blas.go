// +build !omitblas

/* interface to Basic Linear Algebra Subprograms (blas).

see http://www.netlib.org/blas/
*/
package blas

// #cgo LDFLAGS: -L/usr/lib/libblas -lblas
// #include <stdlib.h>
// #include "blas.h"
import "C"

import (
	"unsafe"
)

func Implemented() bool {
	return true
}

//extern double dasum_(int *n, double *x, int *incx);
func Dasum(n int, x []float64, incx int) float64 {
	r := C.dasum_(
		(*C.int)(unsafe.Pointer(&n)),
		array64(x),
		(*C.int)(unsafe.Pointer(&incx)),
	)
	return float64(r)
}

//extern void dcopy_(int *n, double *x, int *incx, double *y, int *incy);
func Dcopy(n int, x []float64, incx int, y []float64, incy int) {
	C.dcopy_(
		(*C.int)(unsafe.Pointer(&n)),
		array64(x),
		(*C.int)(unsafe.Pointer(&incx)),
		array64(y),
		(*C.int)(unsafe.Pointer(&incy)),
	)
}

// extern double dnrm2_(int *n, double *x, int *incx);
func Dnrm2(n int, x []float64, incX int) float64 {
	r := C.dnrm2_(
		(*C.int)(unsafe.Pointer(&n)),
		array64(x),
		(*C.int)(unsafe.Pointer(&incX)),
	)
	return float64(r)
}

func array64(x []float64) *C.double {
	var xptr *float64
	if len(x) > 0 {
		xptr = &x[0]
	}
	return (*C.double)(unsafe.Pointer(xptr))
}
func array32(x []float32) *C.float {
	var xptr *float32
	if len(x) > 0 {
		xptr = &x[0]
	}
	return (*C.float)(unsafe.Pointer(xptr))
}

// extern void daxpy_(int *n, double *alpha, double *x, int *incx, double *y, int *incy);
func Dacpy(N int, alpha float64, x []float64, incX int, y []float64, incY int) {
	C.daxpy_(
		(*C.int)(unsafe.Pointer(&N)),
		(*C.double)(unsafe.Pointer(&alpha)),
		(*C.double)(unsafe.Pointer(array64(x))),
		(*C.int)(unsafe.Pointer(&incX)),
		array64(y),
		(*C.int)(unsafe.Pointer(&incY)),
	)
}

func Ddot(N int, X []float64, incX int, Y []float64, incY int) float64 {
	val := C.ddot_((*C.int)(unsafe.Pointer(&N)),
		array64(X),
		(*C.int)(unsafe.Pointer(&incX)),
		array64(Y),
		(*C.int)(unsafe.Pointer(&incY)))
	return float64(val)
}
func Sdot(N int, X []float32, incX int, Y []float32, incY int) float32 {
	val := C.sdot_((*C.int)(unsafe.Pointer(&N)),
		array32(X),
		(*C.int)(unsafe.Pointer(&incX)),
		array32(Y),
		(*C.int)(unsafe.Pointer(&incY)))
	return float32(val)
}

// extern void sgemm_(char *transa, char *transb, int *m, int *n, int *k, float *alpha, float *A, int *lda, float *B, int *ldb, float *beta, float *C, int *ldc);
func Sgemm(transA, transB string, M int, N int, K int,
	alpha float32, A []float32, lda int, B []float32, ldb int, beta float32,
	C []float32, ldc int) {

	if transA != "N" || transB != "N" {
		panic("unimplemented")
	}

	ctransA := C.CString(transA)
	defer C.free(unsafe.Pointer(ctransA))
	ctransB := C.CString(transB)
	defer C.free(unsafe.Pointer(ctransB))

	C.sgemm_(ctransA, ctransB,
		(*C.int)(unsafe.Pointer(&M)),
		(*C.int)(unsafe.Pointer(&N)),
		(*C.int)(unsafe.Pointer(&K)),
		(*C.float)(unsafe.Pointer(&alpha)),
		array32(A),
		(*C.int)(unsafe.Pointer(&lda)),
		array32(B),
		(*C.int)(unsafe.Pointer(&ldb)),
		(*C.float)(unsafe.Pointer(&beta)),
		array32(C),
		(*C.int)(unsafe.Pointer(&ldc)))
}

//extern void dsbmv_(char *uplo, int *n, int *k, double *alpha, double *A,
//    int *lda, double *x, int *incx, double *beta, double *y, int *incy);

func Dsbmv(uplo string, n, k int, alpha float64, a []float64, lda int, x []float64, incx int, beta float64, y []float64, incy int) {
	cuplo := C.CString(uplo)
	defer C.free(unsafe.Pointer(cuplo))

	C.dsbmv_(
		cuplo,
		(*C.int)(unsafe.Pointer(&n)),
		(*C.int)(unsafe.Pointer(&k)),
		(*C.double)(unsafe.Pointer(&alpha)),
		array64(a),
		(*C.int)(unsafe.Pointer(&lda)),
		array64(x),
		(*C.int)(unsafe.Pointer(&incx)),
		(*C.double)(unsafe.Pointer(&beta)),
		array64(y),
		(*C.int)(unsafe.Pointer(&incy)),
	)
}

func Dgemm(transA, transB string, M int, N int, K int,
	alpha float64, A []float64, lda int, B []float64, ldb int, beta float64,
	C []float64, ldc int) {

	if transA != "N" || transB != "N" {
		panic("unimplemented")
	}

	ctransA := C.CString(transA)
	defer C.free(unsafe.Pointer(ctransA))
	ctransB := C.CString(transB)
	defer C.free(unsafe.Pointer(ctransB))

	C.dgemm_(ctransA, ctransB,
		(*C.int)(unsafe.Pointer(&M)),
		(*C.int)(unsafe.Pointer(&N)),
		(*C.int)(unsafe.Pointer(&K)),
		(*C.double)(unsafe.Pointer(&alpha)),
		array64(A),
		(*C.int)(unsafe.Pointer(&lda)),
		array64(B),
		(*C.int)(unsafe.Pointer(&ldb)),
		(*C.double)(unsafe.Pointer(&beta)),
		array64(C),
		(*C.int)(unsafe.Pointer(&ldc)))
}

func Dgemv(transA string, M int, N int, alpha float64,
	A []float64, lda int, X []float64, incX int, beta float64,
	Y []float64, incY int) {

	ctransA := C.CString(transA)
	defer C.free(unsafe.Pointer(ctransA))

	C.dgemv_(ctransA,
		(*C.int)(unsafe.Pointer(&M)),
		(*C.int)(unsafe.Pointer(&N)),
		(*C.double)(unsafe.Pointer(&alpha)),
		array64(A),
		(*C.int)(unsafe.Pointer(&lda)),
		array64(X),
		(*C.int)(unsafe.Pointer(&incX)),
		(*C.double)(unsafe.Pointer(&beta)),
		array64(Y),
		(*C.int)(unsafe.Pointer(&incY)))
}
