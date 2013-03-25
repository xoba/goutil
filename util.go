package goutil

import (
	"bytes"
	"io"
	"math/rand"
	"runtime"
	"time"
)

// generic platform init, taking full advantage of all cpu's
func PlatformInit() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())
}

// e.g., can combine retry logic and backoff in time together, or any other strategy
type RetryStrategy interface {
	// returns whether or not to retry
	Retry() bool
}

type RetryBackoffStrat struct {
	Delay         time.Duration
	Retries       int
	BackoffFactor float64
	count         int
}

func (r *RetryBackoffStrat) Retry() bool {
	if r.count < r.Retries {
		r.count++
		SleepRand(r.Delay)
		if r.BackoffFactor == 0.0 {
			r.BackoffFactor = 3.0
		}
		r.Delay = time.Duration(int64(r.BackoffFactor * float64(r.Delay)))
		return true
	}
	return false
}

/*
 sleeps the given amount of time, and then some similarly scaled amount, randomly
*/
func SleepRand(t time.Duration) {
	time.Sleep(t + time.Duration(rand.Int63n(int64(t))))
}

// retries something, generically
func Retry(bs RetryStrategy, f func() (interface{}, error)) (v interface{}, err error) {
	for {
		v, err = f()
		if err == nil {
			return
		}
		if !bs.Retry() {
			return
		}
	}
}

type ReaderFactory interface {
	CreateReader() (io.Reader, error)
}

type BufferReaderFact struct {
	Buffer []byte
}

func (b BufferReaderFact) CreateReader() (io.Reader, error) {
	return bytes.NewReader(b.Buffer), nil
}
