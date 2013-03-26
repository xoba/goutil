package awsmr

import (
	"bytes"
	"github.com/xoba/goutil/aws/s3"
	"github.com/xoba/goutil/mr"
	"io"
	"sync"
)

type BucketDriver struct {
	Loaders        int
	Bucket, Prefix string
	S3             s3.Interface
}

func (d BucketDriver) Drive(output chan<- mr.KeyValue) {
	wg := sync.WaitGroup{}
	keys := make(chan string, 1000)
	for i := 0; i < d.Loaders; i++ {
		wg.Add(1)
		go loaderRoutine(&wg, d.S3, d.Bucket, keys, output)
	}
	var marker string
	for {
		r, err := d.S3.List(s3.ListRequest{MaxKeys: 1000, Bucket: d.Bucket, Prefix: d.Prefix, Marker: marker})
		if err != nil {
			panic(err)
		}
		for _, v := range r.Contents {
			keys <- v.Key
		}
		if r.IsTruncated {
			marker = r.Contents[len(r.Contents)-1].Key
		} else {
			break
		}
	}
	close(keys)
	wg.Wait()
	close(output)
}

func fetchFromS3(ss3 s3.Interface, bucket, key string, input chan<- mr.KeyValue) error {
	r, err := ss3.Get(s3.GetRequest{s3.Object{Bucket: bucket, Key: key}})
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	io.Copy(&buf, r)
	input <- createKV(key, buf.Bytes())
	return nil
}

func createKV(name string, buf []byte) mr.KeyValue {
	v := make(map[string]interface{})
	v["content"] = buf
	return mr.KeyValue{Key: name, Value: v}
}

func loaderRoutine(wg *sync.WaitGroup, ss3 s3.Interface, bucket string, keys chan string, input chan<- mr.KeyValue) {
	for key := range keys {
		fetchFromS3(ss3, bucket, key, input)
	}
	wg.Done()
}
