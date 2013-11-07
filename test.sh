#!/bin/bash
go test -cover github.com/xoba/goutil/gmath/blas github.com/xoba/goutil/gmath/stats github.com/xoba/goutil/gmath/la github.com/xoba/goutil/aws/s3 github.com/xoba/goutil/aws/ddb
exit $?
