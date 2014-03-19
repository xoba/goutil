// various utilities for go-lang.
package goutil

import (
	"archive/zip"
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	Version = "1.0"
)

// generic platform init, taking full advantage of all cpu's
func PlatformInit() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
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

func InTimeRange(t, startInclusive, endExclusive time.Time) bool {
	return t.Equal(startInclusive) || (t.After(startInclusive) && t.Before(endExclusive))
}

func NewSingleReaderFact(r io.Reader, length int) ReaderFactory {
	return &srf{length: length, reader: r}
}

type srf struct {
	created bool
	length  int
	reader  io.Reader
}

func (r *srf) CreateReader() (io.ReadCloser, error) {
	if r.created {
		return nil, fmt.Errorf("already created")
	} else {
		r.created = true
		return &ReadCloser{r.reader}, nil
	}
}
func (r *srf) Len() int {
	return r.length
}

type ReadCloser struct {
	io.Reader
}

func (r ReadCloser) Close() error {
	return nil
}

func MarshalIndent(i interface{}) string {
	if buf, err := json.MarshalIndent(i, "", "  "); err == nil {
		return string(buf)
	} else {
		return fmt.Sprintf("%v", i)
	}
}

func Marshal(i interface{}) string {
	if buf, err := json.Marshal(i); err == nil {
		return string(buf)
	} else {
		return fmt.Sprintf("%v", i)
	}
}

type TimeList []time.Time

func (r TimeList) Len() int {
	return len(r)
}
func (r TimeList) Less(i, j int) bool {
	return r[i].Before(r[j])
}
func (r TimeList) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

// ----------------------------------------

type KeyValueF struct {
	Key   string
	Value float64
}
type KeyValueListF []KeyValueF

func (k KeyValueListF) Len() int {
	return len(k)
}
func (k KeyValueListF) Less(i, j int) bool {
	return k[i].Value < k[j].Value
}

func (k KeyValueListF) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

// ----------------------------------------

type KeyValueI struct {
	Key   string
	Value int
}
type KeyValueListI []KeyValueI

func (k KeyValueListI) Len() int {
	return len(k)
}
func (k KeyValueListI) Less(i, j int) bool {
	return k[i].Value < k[j].Value
}

func (k KeyValueListI) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

// ----------------------------------------

type Filter interface {
	Update(t time.Time, x float64) float64
}

func NewLowPass(tc time.Duration) Filter {
	return &LowPass{
		tc:    tc,
		first: true,
	}
}

type LowPass struct {
	first bool
	tc    time.Duration

	lastTime time.Time

	lastOut      float64
	currentValue float64
}

func (f *LowPass) Update(t time.Time, x float64) float64 {
	if f.first {
		f.currentValue = 0
		f.first = false
	} else {
		dt := t.Sub(f.lastTime)
		if dt > 0 {
			tc0 := float64(f.tc) / float64(dt)
			a := 1.0 / (tc0 + 1)
			f.currentValue = a*x + (1-a)*f.lastOut
		}
	}
	f.lastOut = f.currentValue
	f.lastTime = t
	return f.currentValue
}

type RateEstimator struct {
	lpf   Filter
	last  time.Time
	count int
	rate  float64
}

func NewRateEstimator(tc time.Duration) *RateEstimator {
	return &RateEstimator{
		lpf: NewLowPass(tc),
	}
}

func (re *RateEstimator) Count() int {
	return re.count
}
func (re *RateEstimator) Rate() float64 {
	return re.rate
}

// updates and return Rate()
func (re *RateEstimator) Update() float64 {
	re.count++
	now := time.Now()
	if !re.last.IsZero() {
		r := 1.0 / now.Sub(re.last).Seconds()
		re.rate = re.lpf.Update(now, r)
	}
	re.last = now
	return re.rate
}

func LoadZipData(path, url string) (*zip.Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		tmp := uuid.New() + ".zip"
		defer os.Remove(tmp)
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		f, err := os.Create(tmp)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if _, err := io.Copy(f, resp.Body); err != nil {
			return nil, err
		}
		err = os.Rename(tmp, path)
		if err != nil {
			return nil, err
		}
		return LoadZipData(path, url)
	} else {
		fi, err := f.Stat()
		if err != nil {
			return nil, err
		}
		return zip.NewReader(f, fi.Size())
	}
}

// runs f and returns output or timeout with error (i.e., it's ok to call like "_,err := Timeout(...)" if you don't care which)
func Timeout(f func() error, t time.Duration) (timedout bool, err error) {
	done := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			done <- err
		}()
		err = f()
	}()
	select {
	case <-time.After(t):
		return true, fmt.Errorf("timed out after %v", t)
	case err := <-done:
		return false, err
	}
}

const IsoFormat = "2006-01-02T15:04:05.000Z"

// formats a time as iso 8601 with IsoFormat
func FormatIsoUtc(t time.Time) string {
	return t.UTC().Format(IsoFormat)
}

// parses a time as iso 8601 with IsoFormat
func ParseIsoUtc(t string) (time.Time, error) {
	return time.ParseInLocation(IsoFormat, t, time.UTC)
}
