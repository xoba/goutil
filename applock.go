package goutil

import (
	"fmt"
	"net/http"
	"time"
)

func StartHttp(port int, message string) {

	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", port),
		Handler:        &handler{message: message, started: time.Now()},
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	go func() {
		check(s.ListenAndServe())
	}()
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
