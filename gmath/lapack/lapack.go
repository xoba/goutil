// +build !omitblas

package lapack

// #cgo LDFLAGS: -L/usr/lib/libblas -L/usr/lib/lapack -llapack -lblas
// #include <stdlib.h>
// #include "lapack.h"
import "C"

import (
	"unsafe"
)

func Sgesdd(jobz string, M, N int, A []float32, lda int, S []float32, U []float32,
	ldu int, Vt []float32, ldvt int) int {

	var info int = 0
	var lwork int = -1
	var work float32

	cjobz := C.CString(jobz)
	defer C.free(unsafe.Pointer(cjobz))

	iwork := make([]int, 8*min(M, N))

	// pre-calculate work buffer size
	C.sgesdd_(cjobz, (*C.int)(unsafe.Pointer(&M)), (*C.int)(unsafe.Pointer(&N)),
		nil, (*C.int)(unsafe.Pointer(&lda)),
		nil, nil, (*C.int)(unsafe.Pointer(&ldu)),
		nil, (*C.int)(unsafe.Pointer(&ldvt)),
		(*C.float)(unsafe.Pointer(&work)), (*C.int)(unsafe.Pointer(&lwork)),
		(*C.int)(unsafe.Pointer(&iwork[0])),
		(*C.int)(unsafe.Pointer(&info)))

	// allocate work area
	lwork = int(work)
	wbuf := make([]float32, lwork)

	var Ubuf, Vtbuf *C.float
	if U != nil {
		Ubuf = (*C.float)(unsafe.Pointer(&U[0]))
	} else {
		Ubuf = (*C.float)(unsafe.Pointer(nil))
	}
	if Vt != nil {
		Vtbuf = (*C.float)(unsafe.Pointer(&Vt[0]))
	} else {
		Vtbuf = (*C.float)(unsafe.Pointer(nil))
	}

	C.sgesdd_(cjobz, (*C.int)(unsafe.Pointer(&M)), (*C.int)(unsafe.Pointer(&N)),
		(*C.float)(unsafe.Pointer(&A[0])), (*C.int)(unsafe.Pointer(&lda)),
		(*C.float)(unsafe.Pointer(&S[0])), Ubuf, (*C.int)(unsafe.Pointer(&ldu)),
		Vtbuf, (*C.int)(unsafe.Pointer(&ldvt)),
		(*C.float)(unsafe.Pointer(&wbuf[0])), (*C.int)(unsafe.Pointer(&lwork)),
		(*C.int)(unsafe.Pointer(&iwork[0])),
		(*C.int)(unsafe.Pointer(&info)))

	return info
}

func Dgesdd(jobz string, M, N int, A []float64, lda int, S []float64, U []float64,
	ldu int, Vt []float64, ldvt int) int {

	var info int = 0
	var lwork int = -1
	var work float64

	cjobz := C.CString(jobz)
	defer C.free(unsafe.Pointer(cjobz))

	iwork := make([]int, 8*min(M, N))

	// pre-calculate work buffer size
	C.dgesdd_(cjobz, (*C.int)(unsafe.Pointer(&M)), (*C.int)(unsafe.Pointer(&N)),
		nil, (*C.int)(unsafe.Pointer(&lda)),
		nil, nil, (*C.int)(unsafe.Pointer(&ldu)),
		nil, (*C.int)(unsafe.Pointer(&ldvt)),
		(*C.double)(unsafe.Pointer(&work)), (*C.int)(unsafe.Pointer(&lwork)),
		(*C.int)(unsafe.Pointer(&iwork[0])),
		(*C.int)(unsafe.Pointer(&info)))

	// allocate work area
	lwork = int(work)
	wbuf := make([]float64, lwork)

	var Ubuf, Vtbuf *C.double
	if U != nil {
		Ubuf = (*C.double)(unsafe.Pointer(&U[0]))
	} else {
		Ubuf = (*C.double)(unsafe.Pointer(nil))
	}
	if Vt != nil {
		Vtbuf = (*C.double)(unsafe.Pointer(&Vt[0]))
	} else {
		Vtbuf = (*C.double)(unsafe.Pointer(nil))
	}

	C.dgesdd_(cjobz, (*C.int)(unsafe.Pointer(&M)), (*C.int)(unsafe.Pointer(&N)),
		(*C.double)(unsafe.Pointer(&A[0])), (*C.int)(unsafe.Pointer(&lda)),
		(*C.double)(unsafe.Pointer(&S[0])), Ubuf, (*C.int)(unsafe.Pointer(&ldu)),
		Vtbuf, (*C.int)(unsafe.Pointer(&ldvt)),
		(*C.double)(unsafe.Pointer(&wbuf[0])), (*C.int)(unsafe.Pointer(&lwork)),
		(*C.int)(unsafe.Pointer(&iwork[0])),
		(*C.int)(unsafe.Pointer(&info)))

	return info
}

func min(i, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}
