// rate-limited reader.
package rl

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// meant for sessions of limited duration, since we're measuring bandwidth from start, not most-recently;
// so, feedback algorithm is probably not so stable in the long term.
type RateLimiter struct {
	start    time.Time
	total    int
	bps      float64
	reader   io.Reader
	minSleep float64
	packets  int
}

const debug = false

func NewRateLimiter(bps float64, r io.Reader) *RateLimiter {
	return &RateLimiter{
		start:    time.Now(),
		reader:   r,
		bps:      bps,
		minSleep: 0.03,
	}
}

func (r *RateLimiter) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if err != nil {
		return
	}
	r.packets++
	r.total += n
	now := time.Now()
	rate := float64(8*r.total) / now.Sub(r.start).Seconds()
	if rate > r.bps {
		seconds := float64(8*r.total)/r.bps - now.Sub(r.start).Seconds()
		if seconds >= r.minSleep {
			if debug {
				fmt.Printf("%s; sleeping %.1fms after %d b / %.1f mb, %d packets; rate = %.3f mb/s\n", now.Format("2006-01-02T15:04:05.000Z"), 1000*seconds, n, float64(r.total)/1000000, r.packets, rate/1000000)
			}
			time.Sleep(time.Duration(seconds * float64(time.Second)))
		}
	}
	return
}

type Test struct {
}

func (m *Test) Name() string {
	return "rl,play with rate limiting"
}
func (m *Test) Run(args []string) {
	resp, err := http.Get("http://dvpub.s3.amazonaws.com/rand.dat")
	check(err)
	defer resp.Body.Close()

	r := NewRateLimiter(20000000, resp.Body)

	Timing(r)
}

func Timing(r io.Reader) {
	start := time.Now()
	n, err := io.Copy(ioutil.Discard, r)
	check(err)
	end := time.Now()
	rate := float64(8*n) / end.Sub(start).Seconds()
	fmt.Printf("%.2f mb @ %.3f mbps\n", float64(n)/1000000, rate/1000000)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
