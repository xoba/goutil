package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/xoba/goutil"
	"github.com/xoba/goutil/aws"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"time"
)

const (
	N = "\n"
)

type Interface interface {
	Put(req PutRequest) error
	Get(req GetRequest) (io.ReadCloser, error)
	List(req ListRequest) (ListBucketResult, error)
	Delete(req DeleteRequest) error
}

type GetRequest struct {
	Object Object
}

type PutRequest struct {
	Object        Object
	ContentType   string
	ContentLength uint64
	ReaderFact    goutil.ReaderFactory
}

type ListRequest struct {
	Bucket  string
	MaxKeys int64
	Marker  string
	Prefix  string
}

type DeleteRequest struct {
	Object Object
}

type Object struct {
	Bucket string
	Key    string
}

type ListBucketResultContents struct {
	Key, ETag, StorageClass string
	Size                    int
	Owner                   ListBucketResultOwner
	LastModified            time.Time
}

type ListBucketResultOwner struct {
	ID, DisplayName string
}

type ListBucketResult struct {
	Name, Prefix, Marker, Delimiter string
	MaxKeys                         int64
	IsTruncated                     bool
	Contents                        []ListBucketResultContents
}

func GetDefault(a aws.Auth) Interface {
	return SmartS3{Auth: a, Strat: &goutil.RetryBackoffStrat{Delay: time.Second, Retries: 3}}
}

type SmartS3 struct {
	Auth  aws.Auth
	Strat goutil.RetryStrategy
}

func (s SmartS3) retry(f func() (interface{}, error)) (v interface{}, err error) {
	return goutil.Retry(s.Strat, f)
}

func (s SmartS3) List(req ListRequest) (ListBucketResult, error) {
	f := func() (interface{}, error) {
		return list(s.Auth, req)
	}
	v, err := s.retry(f)
	if err != nil {
		return ListBucketResult{}, err
	} else {
		return v.(ListBucketResult), err
	}
}

func (s SmartS3) Get(req GetRequest) (io.ReadCloser, error) {
	f := func() (interface{}, error) {
		return get(s.Auth, req)
	}
	v, err := s.retry(f)
	if err != nil {
		return nil, err
	} else {
		return v.(io.ReadCloser), err
	}
}

func (s SmartS3) Put(req PutRequest) error {
	f := func() (interface{}, error) {
		return nil, put(s.Auth, req)
	}
	_, err := s.retry(f)
	return err
}

func (s SmartS3) Delete(req DeleteRequest) error {
	f := func() (interface{}, error) {
		return nil, del(s.Auth, req)
	}
	_, err := s.retry(f)
	return err
}

func mimeType(name string) string {
	ext := filepath.Ext(name)
	return mime.TypeByExtension(ext)
}

func list(auth aws.Auth, req ListRequest) (out ListBucketResult, err error) {
	if req.Bucket == "" {
		return out, errors.New("no bucket name")
	}
	query := make(url.Values)
	if req.MaxKeys > 0 {
		query.Add("max-keys", fmt.Sprintf("%d", req.MaxKeys))
	} else {
		query.Add("max-keys", "1000")
	}
	if req.Marker != "" {
		query.Add("marker", req.Marker)
	}
	if req.Prefix != "" {
		query.Add("prefix", req.Prefix)
	}
	now := time.Now()
	sig, err := signList(req, auth, now)
	if err != nil {
		return
	}
	transport := http.DefaultTransport
	url := "https://" + req.Bucket + ".s3.amazonaws.com/?" + query.Encode()
	hreq, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	hreq.Header.Add("Date", format(now))
	hreq.Header.Add("Authorization", "AWS "+auth.AccessKey+":"+sig)
	resp, err := transport.RoundTrip(hreq)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return out, errors.New(resp.Status)
	}
	var buf bytes.Buffer
	io.Copy(&buf, resp.Body)
	xml.Unmarshal(buf.Bytes(), &out)
	return
}

func del(auth aws.Auth, req DeleteRequest) (err error) {
	now := time.Now()
	sig, err := signDelete(req, auth, now)
	if err != nil {
		return
	}
	transport := http.DefaultTransport
	hreq, err := http.NewRequest("DELETE", "https://"+req.Object.Bucket+".s3.amazonaws.com/"+req.Object.Key, nil)
	if err != nil {
		return err
	}
	hreq.Header.Add("Date", format(now))
	hreq.Header.Add("Authorization", "AWS "+auth.AccessKey+":"+sig)
	resp, err := transport.RoundTrip(hreq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}
	return nil
}

func get(auth aws.Auth, req GetRequest) (rc io.ReadCloser, err error) {
	now := time.Now()
	sig, err := signGet(req, auth, now)
	if err != nil {
		return
	}
	transport := http.DefaultTransport
	hreq, err := http.NewRequest("GET", "https://"+req.Object.Bucket+".s3.amazonaws.com/"+req.Object.Key, nil)
	if err != nil {
		return nil, err
	}
	hreq.Header.Add("Date", format(now))
	hreq.Header.Add("Authorization", "AWS "+auth.AccessKey+":"+sig)
	resp, err := transport.RoundTrip(hreq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
	}
	return resp.Body, nil
}

func format(t time.Time) string {
	return t.UTC().Format(time.RFC1123Z)
}

func put(auth aws.Auth, req PutRequest) (err error) {
	now := time.Now()
	transport := http.DefaultTransport
	reader, err := req.ReaderFact.CreateReader()
	if err != nil {
		return err
	}
	hreq, err := http.NewRequest("PUT", "https://"+req.Object.Bucket+".s3.amazonaws.com/"+req.Object.Key, reader)
	if err != nil {
		return err
	}
	hreq.Header.Add("Date", format(now))
	if len(req.ContentType) == 0 {
		req.ContentType = mimeType(req.Object.Key)
	}
	sig, err := signPut(req, auth, now)
	if err != nil {
		return
	}
	hreq.Header.Add("Content-Type", req.ContentType)
	hreq.Header.Add("Content-Length", string(req.ContentLength))
	hreq.Header.Add("Authorization", "AWS "+auth.AccessKey+":"+sig)
	resp, err := transport.RoundTrip(hreq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}
	return nil
}

func signPut(r PutRequest, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "PUT"+N+N+r.ContentType+N+format(t)+N+"/"+r.Object.Bucket+"/"+r.Object.Key)
}

func signList(r ListRequest, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "GET"+N+N+N+format(t)+N+"/"+r.Bucket+"/")
}

func signGet(r GetRequest, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "GET"+N+N+N+format(t)+N+"/"+r.Object.Bucket+"/"+r.Object.Key)
}
func signDelete(r DeleteRequest, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "DELETE"+N+N+N+format(t)+N+"/"+r.Object.Bucket+"/"+r.Object.Key)
}

func sign(a aws.Auth, toSign string) (signature string, err error) {
	h := hmac.New(sha1.New, []byte(a.SecretKey))
	if _, err = h.Write([]byte(toSign)); err != nil {
		return
	}
	sig := h.Sum(nil)
	buf := new(bytes.Buffer)
	encoder := base64.NewEncoder(base64.StdEncoding, buf)
	if _, err = encoder.Write(sig); err != nil {
		return
	}
	if err = encoder.Close(); err != nil {
		return
	}
	signature = buf.String()
	return
}
