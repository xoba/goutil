package goutil

import (
	"compress/gzip"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

// returns whether request is authenticated or not, and if not, optionally uses w to aid authentication
type Authenticator func(w http.ResponseWriter, r *http.Request) bool

// returns either cert/key files or bytes, or nulls if ssl not needed
type SSLConfig func() (cert interface{}, key interface{})

// simple generic platform, with gzip handling
func RunWeb(handler http.Handler, port int, ssl SSLConfig, auth Authenticator) error {
	if err := mime.AddExtensionType(".ttf", "application/x-font-ttf"); err != nil {
		return err
	}
	s := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: makeGzipHandler(makeAuthHandler(handler, auth)),
	}
	cert, key := ssl()
	if cert == nil {
		return s.ListenAndServe()
	} else {
		switch t := cert.(type) {
		case string:
			return s.ListenAndServeTLS(t, key.(string))
		case []byte:
			return ListenAndServeTLS(s, t, key.([]byte))
		default:
			return fmt.Errorf("illegal type for ssl: %T", t)
		}
	}
}

type RedirectorFunc func(r *http.Request) string

// redirects http to https; rf can be nil
func RunSSLRedirector(port int, rf RedirectorFunc) error {
	s := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: &Redirector{rf},
	}
	return s.ListenAndServe()
}

type Redirector struct {
	Transformer RedirectorFunc
}

func (f Redirector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if f.Transformer != nil {
		http.Redirect(w, r, f.Transformer(r), 307)
	} else {
		u := r.URL
		u.Host = r.Host
		u.Scheme = "https"
		http.Redirect(w, r, u.String(), 307)
	}
}

func makeAuthHandler(h http.Handler, auth Authenticator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth(w, r) {
			h.ServeHTTP(w, r)
		}
	})
}

// adapted from https://gist.github.com/the42/1956518

func makeGzipHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			h.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzr := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		h.ServeHTTP(gzr, r)
	})
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	if "" == w.Header().Get("Content-Type") {
		w.Header().Set("Content-Type", http.DetectContentType(b))
	}
	return w.Writer.Write(b)
}
