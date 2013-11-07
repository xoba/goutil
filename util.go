// various utilities for go-lang.
package goutil

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strings"
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

func DataUrl(b []byte, mimeType string) string {
	b64 := base64.StdEncoding.EncodeToString(b)
	return fmt.Sprintf("data:%s;base64,%s", mimeType, b64)
}

type RetryStrategyInstance interface {
	// returns whether or not to retry, backing off in time if it likes
	Retry() bool
}

type NoRetryStrategy struct {
}
type NoRetryStrategyInstance struct {
}

func (NoRetryStrategyInstance) Retry() bool {
	return false
}
func (NoRetryStrategy) NewInstance() RetryStrategyInstance {
	return NoRetryStrategyInstance{}
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
	Len() int
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

func (b BufferReaderFact) Len() int {
	return len(b.Buffer)
}

func (b BufferReaderFact) CreateReader() (io.ReadCloser, error) {
	buf := bytes.NewBuffer(b.Buffer)
	return BufferReader{buf}, nil
}

func NewFileReaderFact(path string) (*FileReaderFact, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &FileReaderFact{Path: path, Length: int(fi.Size())}, nil
}

type FileReaderFact struct {
	Path   string
	Length int
}

func (rf FileReaderFact) Len() int {
	return rf.Length
}

func (rf FileReaderFact) CreateReader() (io.ReadCloser, error) {
	f, err := os.Open(rf.Path)
	if err != nil {
		return nil, err
	}
	return f, nil
}

type Authorizer func(r *http.Request) bool

type HttpAuth struct {
	authorizers []Authorizer
	realm       string
	expecting   map[string]string
	handler     http.Handler
}

func NewHttpAuth(realm string, handler http.Handler, auths ...Authorizer) *HttpAuth {
	return &HttpAuth{realm: realm, handler: handler, expecting: make(map[string]string), authorizers: auths}
}

func (a *HttpAuth) Add(user, password string) {
	var dst bytes.Buffer
	enc := base64.NewEncoder(base64.StdEncoding, &dst)
	str := user + ":" + password
	enc.Write([]byte(str))
	enc.Close()
	a.expecting[string(dst.Bytes())] = str
}

func (a *HttpAuth) Authorized(r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	parts := strings.Split(auth, " ")
	if len(parts) == 2 {
		u, ok := a.expecting[parts[1]]
		if ok {
			r.Header.Set("X-Authorization-Decoded", u)
		}
		return ok
	}
	return false
}

func (s *HttpAuth) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	authorized := func() bool {
		if s.Authorized(r) {
			return true
		}
		for _, x := range s.authorizers {
			if x(r) {
				return true
			}
		}
		return false
	}()

	if authorized {
		s.handler.ServeHTTP(w, r)
	} else {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Basic realm="%s"`, s.realm))
		w.Header().Set("Content-Type", `text/plain`)
		w.WriteHeader(401)
		fmt.Fprintf(w, "please authenticate!\n")
	}
}

type HttpAuthMux struct {
	realm     string
	expecting map[string]string
	mux       *http.ServeMux
}

func NewHttpAuthMux(realm string) *HttpAuthMux {
	return &HttpAuthMux{realm: realm, mux: http.NewServeMux(), expecting: make(map[string]string)}
}

func (a *HttpAuthMux) Add(user, password string) {
	var dst bytes.Buffer
	enc := base64.NewEncoder(base64.StdEncoding, &dst)

	str := user + ":" + password

	enc.Write([]byte(str))
	enc.Close()

	a.expecting[string(dst.Bytes())] = str
}

func (a *HttpAuthMux) Authorized(r *http.Request) bool {

	auth := r.Header.Get("Authorization")

	parts := strings.Split(auth, " ")

	if len(parts) == 2 {
		_, ok := a.expecting[parts[1]]
		return ok
	}

	return false
}

func (s *HttpAuthMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if s.Authorized(r) {
		s.mux.ServeHTTP(w, r)
	} else {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Basic realm="%s"`, s.realm))
		w.Header().Set("Content-Type", `text/plain`)
		w.WriteHeader(401)
		fmt.Fprintf(w, "please authenticate!\n")
	}

}

func (s *HttpAuthMux) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

func (s *HttpAuthMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.mux.HandleFunc(pattern, handler)
}
