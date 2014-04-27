// code for accessing s3.
package s3

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/xoba/goutil"
	"github.com/xoba/goutil/aws"
)

type Interface interface {
	Copy(req CopyRequest) error
	Put(req PutRequest) error
	PutObject(req PutObjectRequest) error
	Head(req Object) (*HeadResponse, error)
	Get(req GetRequest) (io.ReadCloser, error)
	GetObject(req GetRequest) ([]byte, error)
	List(req ListRequest) (ListBucketResult, error)
	Delete(req DeleteRequest) error
	Buckets() (*ListAllMyBucketsResult, error)
	MakePublic(bucket string) error
}

func GetDefault(a aws.Auth) Interface {
	return SmartS3{Auth: a, Strat: &goutil.RetryBackoffStrat{BackoffFactor: 1.5, Delay: time.Second, Retries: 3}}
}

type ListAllMyBucketsResult struct {
	Owner   Owner
	Buckets []Bucket `xml:">Bucket"`
}

type Owner struct {
	ID, DisplayName string
}

type Bucket struct {
	Name         string
	CreationDate string
}

type HeadResponse struct {
	ETag          string
	ContentType   string
	ContentLength int
	LastModified  time.Time
}

type GetRequest struct {
	Object       Object
	RoundTripper http.RoundTripper
}

type CopyRequest struct {
	From, To Object
}

type BasePut struct {
	Object          Object
	ContentType     string
	ContentEncoding string
	ContentMD5      string // md5, hex or base64
}

type PutRequest struct {
	BasePut
	ReaderFact goutil.ReaderFactory
}

type PutObjectRequest struct {
	BasePut
	Data []byte
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

func (o Object) Url() string {
	return fmt.Sprintf("https://s3.amazonaws.com/%s/%s", o.Bucket, o.Key)
}

type ListBucketResultContents struct {
	Key, ETag, StorageClass string `json:",omitempty"`
	Size                    int
	LastModified            time.Time `json:",omitempty"`
}

type ListedObject struct {
	ListBucketResultContents
	Bucket string
}

func (b ListedObject) Object() Object {
	return Object{
		Bucket: b.Bucket,
		Key:    b.Key,
	}
}

type ListBucketResult struct {
	Name, Prefix, Marker, Delimiter string
	MaxKeys                         int64
	IsTruncated                     bool
	Contents                        []ListBucketResultContents
}

type SmartS3 struct {
	Auth  aws.Auth
	Strat goutil.RetryStrategy
}

func (s SmartS3) List(req ListRequest) (ListBucketResult, error) {
	var out ListBucketResult
	if req.Bucket == "" {
		return out, errors.New("no bucket name")
	}
	f := func() (interface{}, error) {
		return list(s.Auth, req)
	}
	v, err := s.retry(str(req), f)
	if err != nil {
		return out, err
	} else {
		return v.(ListBucketResult), err
	}
}

func (s SmartS3) Get(req GetRequest) (io.ReadCloser, error) {
	err := checkObject(req.Object)
	if err != nil {
		return nil, err
	}
	f := func() (interface{}, error) {
		return get(s.Auth, req)
	}
	v, err := s.retry(str(req), f)
	if err != nil {
		return nil, err
	} else {
		return v.(io.ReadCloser), err
	}
}

func (s SmartS3) MakePublic(bucket string) error {
	policy := fmt.Sprintf(`{"Statement":[{"Action":"s3:GetObject","Effect":"Allow","Principal":{"AWS":"*"},"Resource":"arn:aws:s3:::%s/*","Sid":"AllowPublicRead"}],"Version":"2008-10-17"}`, bucket)
	return putPolicy(s.Auth, bucket, policy)
}

func (s SmartS3) Buckets() (*ListAllMyBucketsResult, error) {
	f := func() (interface{}, error) {
		return buckets(s.Auth)
	}
	v, err := s.retry("service", f)
	if err != nil {
		return nil, err
	} else {
		return v.(*ListAllMyBucketsResult), err
	}
}

func (s SmartS3) Head(req Object) (*HeadResponse, error) {
	err := checkObject(req)
	if err != nil {
		return nil, err
	}
	f := func() (interface{}, error) {
		return head(s.Auth, req)
	}
	v, err := s.retry(str(req), f)
	if err != nil {
		return nil, err
	} else {
		return v.(*HeadResponse), err
	}
}

func (s SmartS3) GetObject(req GetRequest) ([]byte, error) {
	err := checkObject(req.Object)
	if err != nil {
		return nil, err
	}
	f := func() (interface{}, error) {
		return getObject(s.Auth, req)
	}
	v, err := s.retry(str(req), f)
	if err != nil {
		return nil, err
	} else {
		return v.([]byte), err
	}
}

func (s SmartS3) Copy(req CopyRequest) error {
	err := checkObject(req.From)
	if err != nil {
		return err
	}
	err = checkObject(req.To)
	if err != nil {
		return err
	}
	f := func() (interface{}, error) {
		return nil, cp(s.Auth, req)
	}
	_, err = s.retry(str(req), f)
	return err
}

func (s SmartS3) Put(req PutRequest) error {
	err := checkObject(req.Object)
	if err != nil {
		return err
	}
	f := func() (interface{}, error) {
		return nil, SimplePut(s.Auth, req)
	}
	_, err = s.retry(str(req), f)
	return err
}

func (s SmartS3) PutObject(req PutObjectRequest) error {
	return s.Put(PutRequest{
		BasePut:    req.BasePut,
		ReaderFact: goutil.BufferReaderFact{req.Data},
	})
}

func (s SmartS3) Delete(req DeleteRequest) error {
	err := checkObject(req.Object)
	if err != nil {
		return err
	}
	f := func() (interface{}, error) {
		return nil, del(s.Auth, req)
	}
	_, err = s.retry(str(req), f)
	return err
}

func (s SmartS3) retry(msg string, f func() (interface{}, error)) (v interface{}, err error) {
	return goutil.Retry(msg, s.Strat.NewInstance(), f)
}

func checkObject(o Object) error {
	if o.Bucket == "" || o.Key == "" {
		return errors.New("illegal bucket or key")
	}
	return nil
}
