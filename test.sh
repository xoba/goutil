#!/bin/bash
go test github.com/xoba/goutil/gmath/blas github.com/xoba/goutil/gmath/stats github.com/xoba/goutil/gmath/la github.com/xoba/goutil/aws/s3
exit $?
