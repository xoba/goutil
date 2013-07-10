package goutil

import (
	"bytes"
	"io"
	"math/rand"
	"runtime"
	"time"
)

const (
	VERSION = "1.0"
)

// generic platform init, taking full advantage of all cpu's
func PlatformInit() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())
}

// e.g., can combine retry logic and backoff in time together, or any other strategy
type RetryStrategy interface {
	NewInstance() RetryStrategyInstance
}

type RetryStrategyInstance interface {
	// returns whether or not to retry, backing off in time if it likes
	Retry() bool
}

type RetryBackoffStratInstance struct {
	retries int
	factor  float64
	delay   time.Duration
	count   int
}

type RetryBackoffStrat struct {
	Delay         time.Duration
	Retries       int
	BackoffFactor float64
}

func (r RetryBackoffStrat) NewInstance() RetryStrategyInstance {
	f := r.BackoffFactor
	if f < 1.0 {
		f = 1.0
	}
	return &RetryBackoffStratInstance{delay: r.Delay, retries: r.Retries, factor: f}
}

func (r *RetryBackoffStratInstance) Retry() bool {
	if r.count < r.retries {
		r.count++
		SleepRand(r.delay)
		r.delay = time.Duration(int64(r.factor * float64(r.delay)))
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
func Retry(msg string, bs RetryStrategyInstance, f func() (interface{}, error)) (v interface{}, err error) {
	retries := 0
	for {
		v, err = f()
		if err == nil {
			return
		}
		if !bs.Retry() {
			return
		} else {
			retries++
		}
	}
}

type ReaderFactory interface {
	CreateReader() (io.ReadCloser, error)
	Len() uint64
}

type BufferReaderFact struct {
	Buffer []byte
}

type BufferReader struct {
	Buffer *bytes.Buffer
}

func (b BufferReader) Close() error {
	return nil
}
func (b BufferReader) Read(p []byte) (n int, err error) {
	return b.Buffer.Read(p)
}

func (b BufferReaderFact) Len() uint64 {
	return uint64(len(b.Buffer))
}

func (b BufferReaderFact) CreateReader() (io.ReadCloser, error) {
	buf := bytes.NewBuffer(b.Buffer)
	return BufferReader{buf}, nil
}
