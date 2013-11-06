// +build linux

package goutil

import (
	"bufio"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"
)

var gcbh chan interface{} = make(chan interface{})

// prevents an object from getting garbage collected
func GCBlackHole() chan<- interface{} {
	return gcbh
}

func init() {
	go func() {
		var blackHole []interface{}
		for i := range gcbh {
			blackHole = append(blackHole, i)
		}
	}()
}

func GrabAppLock(path string) (locked bool, appId string) {
	locked = ExclusiveLock(path, 3*time.Second, AppIdLocker(func(id string, f *os.File) {
		m := make(map[string]string)
		m["path"] = path
		m["id"] = id
		if buf, err := json.Marshal(m); err == nil {
			appId = string(buf)
		} else {
			appId = id
		}
		GCBlackHole() <- f
	}))
	return
}

// maintains a persistent uuid
func AppIdLocker(callback func(string, *os.File)) func(*os.File) bool {
	return func(f *os.File) bool {
		var lines []string
		f.Seek(0, 0)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		var id string
		if scanner.Err() != nil || len(lines) == 0 {
			f.Seek(0, 0)
			id = uuid.New()
			_, err := fmt.Fprintf(f, "%s\n", id)
			if err != nil {
				return false
			}
			err = f.Sync()
			if err != nil {
				return false
			}
		} else if len(lines) > 0 {
			id = lines[0]
		}
		callback(id, f)
		return true
	}
}

// grab exclusive r/w lock within deadline, callback on successfully locked file
// note that if the file gets garbage collected, lock is released!
func ExclusiveLock(path string, deadline time.Duration, callback func(*os.File) bool) bool {

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return false
	}

	ch := make(chan error)

	go func() {
		ch <- syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
	}()

	select {
	case <-ch:
		if err != nil {
			return false
		}
	case <-time.After(deadline):
		return false
	}

	return callback(file)
}

func StartHttp(port int, message string) error {

	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", port),
		Handler:        &handler{message: message, started: time.Now()},
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	l, e := net.Listen("tcp", s.Addr)
	if e != nil {
		return e
	}

	go func() {
		check(s.Serve(l))
	}()

	return nil
}

type handler struct {
	started time.Time
	message string
}

func (f *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("%s\nup %v\n", f.message, time.Now().Sub(f.started))))
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
