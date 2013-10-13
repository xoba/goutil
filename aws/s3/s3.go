// code for accessing s3.
package s3

import (
	"errors"
	"fmt"
	"github.com/xoba/goutil"
	"github.com/xoba/goutil/aws"
	"io"
	"time"
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
}

func GetDefault(a aws.Auth) Interface {
	return SmartS3{Auth: a, Strat: &goutil.RetryBackoffStrat{BackoffFactor: 1.5, Delay: time.Second, Retries: 5}}
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
	Etag          string
	ContentType   string
	ContentLength int
	LastModified  time.Time
}

type GetRequest struct {
	Object Object
}

type CopyRequest struct {
	From, To Object
}

type PutRequest struct {
	Object      Object
	ContentType string
	ReaderFact  goutil.ReaderFactory
}

type PutObjectRequest struct {
	Object      Object
	ContentType string
	Compress    bool
	Data        []byte
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

func (o Object) ToUrl() string {
	return fmt.Sprintf("https://s3.amazonaws.com/%s/%s", o.Bucket, o.Key)
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
		return nil, put(s.Auth, req)
	}
	_, err = s.retry(str(req), f)
	return err
}

func (s SmartS3) PutObject(req PutObjectRequest) error {
	err := checkObject(req.Object)
	if err != nil {
		return err
	}
	f := func() (interface{}, error) {
		return nil, putObject(s.Auth, req)
	}
	_, err = s.retry(str(req), f)
	return err
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
