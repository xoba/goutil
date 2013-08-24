package s3

import (
	"testing"
)

func TestEmptyS3Object(t *testing.T) {
	if err := checkObject(Object{}); err == nil {
		t.Errorf("failed to detect empty object")
	}
}

func TestEmptyS3Key(t *testing.T) {
	if err := checkObject(Object{Bucket: "blah"}); err == nil {
		t.Errorf("failed to detect empty key")
	}
}
func TestEmptyS3Bucket(t *testing.T) {
	if err := checkObject(Object{Key: "blah"}); err == nil {
		t.Errorf("failed to detect empty bucket")
	}
}
func TestGoodKey(t *testing.T) {
	if err := checkObject(Object{Bucket: "abc", Key: "blah"}); err != nil {
		t.Errorf("failed to recognize valid object")
	}
}
