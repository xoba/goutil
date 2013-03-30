package awsmr

import (
	"github.com/xoba/goutil/aws/s3"
	"github.com/xoba/goutil/mr"
	"math/rand"
	"sync"
)

type BucketDriver struct {
	Loaders        int
	Bucket, Prefix string
	Fraction       float64 // random fraction of keys to load, in range [0.0, 1.0]
	S3             s3.Interface
}

func (d BucketDriver) Drive(output chan<- mr.KeyValue) {
	wg := sync.WaitGroup{}
	keys := make(chan string, d.Loaders)
	for i := 0; i < d.Loaders; i++ {
		wg.Add(1)
		go loaderRoutine(&wg, d.S3, d.Bucket, keys, output, d.Fraction)
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
	buf, err := ss3.GetObject(s3.GetRequest{s3.Object{Bucket: bucket, Key: key}})
	if err != nil {
		return err
	}
	input <- createBinaryKV(key, buf)
	return nil
}

func createBinaryKV(name string, buf []byte) mr.KeyValue {
	v := make(map[string]interface{})
	v["content"] = buf
	return mr.KeyValue{Key: name, Value: v}
}

func loaderRoutine(wg *sync.WaitGroup, ss3 s3.Interface, bucket string, keys chan string, input chan<- mr.KeyValue, fraction float64) {
	if fraction == 0.0 {
		fraction = 1.0
	}
	for key := range keys {
		if rand.Float64() < fraction {
			err := fetchFromS3(ss3, bucket, key, input)
			if err != nil {
				panic(err)
			}
		}
	}
	wg.Done()
}
