package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/xoba/goutil/aws"
)

const (
	N = "\n"
)

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
	u, err := url.Parse("https://s3.amazonaws.com/" + req.Bucket + "/?" + query.Encode())
	now := time.Now()
	sig, err := signList(u.Path, auth, now)
	if err != nil {
		return
	}
	transport := http.DefaultTransport
	hreq, err := http.NewRequest("GET", u.String(), nil)
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
	_, err = io.Copy(&buf, resp.Body)
	if err != nil {
		return
	}
	err = xml.Unmarshal(buf.Bytes(), &out)
	if err != nil {
		return
	}
	return
}

func createURL(o Object) (*url.URL, error) {
	return url.Parse(o.Url())
}

func createURL2() (*url.URL, error) {
	return url.Parse("https://s3.amazonaws.com/")
}

func head(auth aws.Auth, req Object) (*HeadResponse, error) {
	u, err := createURL(req)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	sig, err := signHead(u.Path, auth, now)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport
	hreq, err := http.NewRequest("HEAD", u.String(), nil)
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

	cl, err := strconv.ParseUint(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, err
	}

	etag := resp.Header.Get("Etag")
	etag = strings.Replace(etag, `"`, "", -1)

	t, err := time.ParseInLocation("Mon, 02 Jan 2006 15:04:05 MST", resp.Header.Get("Last-Modified"), time.UTC)
	if err != nil {
		return nil, err
	}

	hr := &HeadResponse{
		ETag:          etag,
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: int(cl),
		LastModified:  t,
	}

	return hr, nil
}

func putPolicy(auth aws.Auth, bucket, policy string) error {
	now := time.Now()
	sig, err := signPut(fmt.Sprintf("/%s/?policy", bucket), "", "", auth, now)
	if err != nil {
		return err
	}
	transport := http.DefaultTransport
	hreq, err := http.NewRequest("PUT", "https://s3.amazonaws.com/?policy", strings.NewReader(policy))
	if err != nil {
		return err
	}
	hreq.Host = fmt.Sprintf("%s.s3.amazonaws.com", bucket)
	hreq.Header.Add("Date", format(now))
	hreq.Header.Add("Authorization", "AWS "+auth.AccessKey+":"+sig)
	resp, err := transport.RoundTrip(hreq)
	if err != nil {
		return err
	}
	io.Copy(os.Stdout, resp.Body)
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}

	return nil
}

func buckets(auth aws.Auth) (*ListAllMyBucketsResult, error) {
	u, err := createURL2()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	sig, err := signGet(u.Path, auth, now)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport
	hreq, err := http.NewRequest("GET", u.String(), nil)
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
	var out ListAllMyBucketsResult
	if false {
		io.Copy(os.Stdout, resp.Body)
	} else {
		d := xml.NewDecoder(resp.Body)
		err = d.Decode(&out)
		if err != nil {
			return nil, err
		}
	}
	return &out, nil
}

func PreSignedUrl(auth aws.Auth, o Object, expiration time.Duration) (string, error) {
	u, err := createURL(o)
	if err != nil {
		return "", err
	}
	now := time.Now()
	t := time.Now().UTC().Add(expiration)
	sig, err := signGetExp(u.Path, auth, now, t)
	return fmt.Sprintf("%s?AWSAccessKeyId=%s&Expires=%d&Signature=%s", o.Url(), url.QueryEscape(auth.AccessKey), t.Unix(), url.QueryEscape(sig)), nil
}

func signGetExp(path string, a aws.Auth, t time.Time, expiration time.Time) (string, error) {
	return sign(a, "GET"+N+N+N+fmt.Sprintf("%d", expiration.Unix())+N+path)
}

func get(auth aws.Auth, req GetRequest) (io.ReadCloser, error) {
	u, err := createURL(req.Object)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	sig, err := signGet(u.Path, auth, now)
	if err != nil {
		return nil, err
	}
	rt := func() http.RoundTripper {
		if req.RoundTripper == nil {
			return http.DefaultTransport
		} else {
			return req.RoundTripper
		}
	}()
	hreq, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	hreq.Header.Add("Date", format(now))
	hreq.Header.Add("Authorization", "AWS "+auth.AccessKey+":"+sig)
	resp, err := rt.RoundTrip(hreq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(resp.Status)
	}
	return resp.Body, nil
}

func del(auth aws.Auth, req DeleteRequest) (err error) {
	u, err := createURL(req.Object)
	if err != nil {
		return err
	}
	now := time.Now()
	sig, err := signDelete(u.Path, auth, now)
	if err != nil {
		return
	}
	transport := http.DefaultTransport
	hreq, err := http.NewRequest("DELETE", u.String(), nil)
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

func getObject(auth aws.Auth, req GetRequest) ([]byte, error) {
	r, err := get(auth, req)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func cp(auth aws.Auth, req CopyRequest) (err error) {
	u, err := createURL(req.To)
	if err != nil {
		return err
	}
	now := time.Now()
	transport := http.DefaultTransport
	hreq, err := http.NewRequest("PUT", u.String(), nil)
	if err != nil {
		return err
	}
	hreq.Header.Add("Date", format(now))
	hreq.Header.Add("x-amz-copy-source", "/"+req.From.Bucket+"/"+req.From.Key)
	sig, err := signCopy(u.Path, auth, now, req.From)
	if err != nil {
		return
	}
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

func signCopy(path string, a aws.Auth, t time.Time, from Object) (string, error) {
	xx := "x-amz-copy-source:/" + from.Bucket + "/" + from.Key
	return sign(a, "PUT"+N+N+N+format(t)+N+xx+N+path)
}

func signPut(path string, md5, ct string, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "PUT"+N+md5+N+ct+N+format(t)+N+path)
}

func SimplePut(auth aws.Auth, req PutRequest) (err error) {
	u, err := createURL(req.Object)
	if err != nil {
		return err
	}
	now := time.Now()
	transport := http.DefaultTransport
	reader, err := req.ReaderFact.CreateReader()
	if err != nil {
		return err
	}
	defer reader.Close()
	hreq, err := http.NewRequest("PUT", u.String(), reader)
	if err != nil {
		return err
	}
	hreq.Header.Add("Date", format(now))
	if len(req.ContentType) == 0 {
		req.ContentType = mimeType(req.Object.Key)
	}

	md5, err := func() (string, error) {
		if len(req.ContentMD5) == 32 {
			d, err := hex.DecodeString(req.ContentMD5)
			if err != nil {
				return "", err
			}
			return base64.StdEncoding.EncodeToString(d), nil
		} else {
			return req.ContentMD5, nil
		}
	}()

	if err != nil {
		return err
	}

	sig, err := signPut(u.Path, md5, req.ContentType, auth, now)
	if err != nil {
		return
	}
	hreq.ContentLength = int64(req.ReaderFact.Len())
	hreq.Header.Add("Content-Type", req.ContentType)
	if len(req.ContentEncoding) > 0 {
		hreq.Header.Add("Content-Encoding", req.ContentEncoding)
	}
	if len(md5) > 0 {
		hreq.Header.Add("Content-MD5", md5)
	}
	hreq.Header.Add("Content-Length", fmt.Sprintf("%d", req.ReaderFact.Len()))
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

func format(t time.Time) string {
	return t.UTC().Format(time.RFC1123Z)
}

func signGet(path string, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "GET"+N+N+N+format(t)+N+path)
}
func signHead(path string, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "HEAD"+N+N+N+format(t)+N+path)
}

func signList(path string, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "GET"+N+N+N+format(t)+N+path)
}

func signDelete(path string, a aws.Auth, t time.Time) (string, error) {
	return sign(a, "DELETE"+N+N+N+format(t)+N+path)
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

func str(v interface{}) string {
	return fmt.Sprintf("%#v", v)
}
