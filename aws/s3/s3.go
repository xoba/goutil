package s3

import (
	"github.com/xoba/goutil"
	"github.com/xoba/goutil/aws"
	"io"
	"time"
)

type Interface interface {
	Put(req PutRequest) error
	PutObject(req PutObjectRequest) error
	Get(req GetRequest) (io.ReadCloser, error)
	GetObject(req GetRequest) ([]byte, error)
	List(req ListRequest) (ListBucketResult, error)
	Delete(req DeleteRequest) error
}

func GetDefault(a aws.Auth) Interface {
	return SmartS3{Auth: a, Strat: &goutil.RetryBackoffStrat{BackoffFactor: 1.5, Delay: time.Second, Retries: 5}}
}

type GetRequest struct {
	Object Object
}

type PutRequest struct {
	Object      Object
	ContentType string
	ReaderFact  goutil.ReaderFactory
}

type PutObjectRequest struct {
	Object      Object
	ContentType string
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
	f := func() (interface{}, error) {
		return list(s.Auth, req)
	}
	v, err := s.retry(print(req), f)
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
	v, err := s.retry(print(req), f)
	if err != nil {
		return nil, err
	} else {
		return v.(io.ReadCloser), err
	}
}
func (s SmartS3) GetObject(req GetRequest) ([]byte, error) {
	f := func() (interface{}, error) {
		return getObject(s.Auth, req)
	}
	v, err := s.retry(print(req), f)
	if err != nil {
		return nil, err
	} else {
		return v.([]byte), err
	}
}

func (s SmartS3) Put(req PutRequest) error {
	f := func() (interface{}, error) {
		return nil, put(s.Auth, req)
	}
	_, err := s.retry(print(req), f)
	return err
}

func (s SmartS3) PutObject(req PutObjectRequest) error {
	f := func() (interface{}, error) {
		return nil, putObject(s.Auth, req)
	}
	_, err := s.retry(print(req), f)
	return err
}

func (s SmartS3) Delete(req DeleteRequest) error {
	f := func() (interface{}, error) {
		return nil, del(s.Auth, req)
	}
	_, err := s.retry(print(req), f)
	return err
}

func (s SmartS3) retry(msg string, f func() (interface{}, error)) (v interface{}, err error) {
	return goutil.Retry(msg, s.Strat.NewInstance(), f)
}
