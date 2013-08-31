// code for accessing dynamo db
package ddb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xoba/goutil"
	"github.com/xoba/goutil/aws"
	"github.com/xoba/goutil/aws4"
	"net/http"
	"strconv"
	"time"
)

type DynamoDB struct {
	Table string
	Auth  aws.Auth
	Strat goutil.RetryStrategy
}

type ValueType byte

const (
	_ = iota
	S
	N
)

type Value struct {
	Type ValueType
	S    string
	N    float64
}

type sType struct {
	S string `json:",omitempty"`
	N string `json:",omitempty"`
}

func createSType(v Value) (out sType) {
	switch v.Type {
	case S:
		out.S = v.S
	case N:
		out.N = fmt.Sprintf("%f", v.N)
	}
	return
}

func SV(s string) Value {
	return Value{Type: S, S: s}
}
func NV(n float64) Value {
	return Value{Type: N, N: n}
}

func (v Value) String() string {
	switch v.Type {
	case S:
		return v.S
	case N:
		return fmt.Sprintf("%f", v.N)
	default:
		return "n/a"
	}
}

func GetDefault(table string, a aws.Auth) DynamoDB {
	return DynamoDB{Table: table, Auth: a, Strat: &goutil.RetryBackoffStrat{BackoffFactor: 2, Delay: 10 * time.Millisecond, Retries: 5}}
}

func (d DynamoDB) UpdateItem(key, attr string, v Value) error {
	f := func() (interface{}, error) {
		err := d.updateItem(key, attr, v)
		return err, err
	}
	_, err := d.retry("update", f)
	return err
}

func (d DynamoDB) IncrementItem(key, attr string, v float64) error {
	f := func() (interface{}, error) {
		err := d.incItem(key, attr, v)
		return err, err
	}
	_, err := d.retry("inc", f)
	return err
}

func (d DynamoDB) DeleteItem(key string) error {
	f := func() (interface{}, error) {
		return nil, d.deleteItem(key)
	}
	_, err := d.retry("delete", f)
	if err != nil {
		return err
	}
	return nil
}

func (d DynamoDB) PutItem(item map[string]Value) error {
	f := func() (interface{}, error) {
		err := d.putItem(item)
		return err, err
	}
	_, err := d.retry("put", f)
	return err
}

type updateRequest struct {
	TableName        string
	Key              keyType
	AttributeUpdates map[string]attrUpdate
}

type attrUpdate struct {
	Value  sType
	Action string
}

func (d DynamoDB) updateItem(key, attr string, v Value) error {

	now := time.Now()

	transport := http.DefaultTransport

	kt := keyType{HashKeyElement: sType{S: key}}

	update := make(map[string]attrUpdate)
	update[attr] = attrUpdate{Value: createSType(v), Action: "PUT"}

	content, err := json.MarshalIndent(updateRequest{TableName: d.Table, Key: kt, AttributeUpdates: update}, "", "  ")

	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", "https://dynamodb.us-east-1.amazonaws.com/", bytes.NewReader(content))

	if err != nil {
		return err
	}

	req.Header.Add("Date", formatTime(now))
	req.Header.Add("X-Amz-Target", "DynamoDB_20111205.UpdateItem")
	req.Header.Add("Content-Type", "application/x-amz-json-1.0")

	keys := d.keys()

	svc := aws4.Service{Name: "dynamodb", Region: "us-east-1"}

	err = svc.Sign(&keys, req)

	if err != nil {
		return err
	}

	resp, err := transport.RoundTrip(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("status " + resp.Status)
	}

	return nil
}

func (d DynamoDB) incItem(key, attr string, v float64) error {

	now := time.Now()

	transport := http.DefaultTransport

	kt := keyType{HashKeyElement: sType{S: key}}

	update := make(map[string]attrUpdate)
	update[attr] = attrUpdate{Value: sType{N: fmt.Sprintf("%f", v)}, Action: "ADD"}

	content, err := json.MarshalIndent(updateRequest{TableName: d.Table, Key: kt, AttributeUpdates: update}, "", "  ")

	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", "https://dynamodb.us-east-1.amazonaws.com/", bytes.NewReader(content))

	if err != nil {
		return err
	}

	req.Header.Add("Date", formatTime(now))
	req.Header.Add("X-Amz-Target", "DynamoDB_20111205.UpdateItem")
	req.Header.Add("Content-Type", "application/x-amz-json-1.0")

	keys := d.keys()

	svc := aws4.Service{Name: "dynamodb", Region: "us-east-1"}

	err = svc.Sign(&keys, req)

	if err != nil {
		return err
	}

	resp, err := transport.RoundTrip(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("status " + resp.Status)
	}

	return nil
}

func (d DynamoDB) putItem(item map[string]Value) error {

	now := time.Now()

	transport := http.DefaultTransport

	m := make(map[string]sType)

	for key, value := range item {

		switch value.Type {
		case S:
			m[key] = sType{S: value.S}
		case N:
			m[key] = sType{N: fmt.Sprintf("%f", value.N)}
		default:
			return errors.New("illegal type")
		}
	}

	content, err := json.MarshalIndent(putRequest{TableName: d.Table, Item: m}, "", "  ")

	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", "https://dynamodb.us-east-1.amazonaws.com/", bytes.NewReader(content))

	if err != nil {
		return err
	}

	req.Header.Add("Date", formatTime(now))
	req.Header.Add("X-Amz-Target", "DynamoDB_20111205.PutItem")
	req.Header.Add("Content-Type", "application/x-amz-json-1.0")

	keys := d.keys()

	svc := aws4.Service{Name: "dynamodb", Region: "us-east-1"}

	err = svc.Sign(&keys, req)

	if err != nil {
		return err
	}

	resp, err := transport.RoundTrip(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("status " + resp.Status)
	}

	return nil
}

func (d DynamoDB) deleteItem(mykey string) error {

	now := time.Now()

	transport := http.DefaultTransport

	myid := sType{S: mykey}
	key := keyType{myid}
	content, err := json.Marshal(deleteRequest{TableName: d.Table, Key: key})

	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", "https://dynamodb.us-east-1.amazonaws.com/", bytes.NewReader(content))
	if err != nil {
		return err
	}

	req.Header.Add("Date", formatTime(now))
	req.Header.Add("X-Amz-Target", "DynamoDB_20111205.DeleteItem")
	req.Header.Add("Content-Type", "application/x-amz-json-1.0")

	keys := d.keys()

	svc := aws4.Service{Name: "dynamodb", Region: "us-east-1"}

	err = svc.Sign(&keys, req)

	if err != nil {
		return err
	}

	resp, err := transport.RoundTrip(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("status " + resp.Status)
	}

	return nil

}

// three return values: item, whether or not item was found, and error if any
func (d DynamoDB) GetItem(key string) (map[string]Value, bool, error) {
	f := func() (interface{}, error) {
		out, has, err := d.getItem(key)
		return getItemResult{Map: out, Has: has}, err
	}
	v, err := d.retry("get", f)
	if err != nil {
		return nil, false, err
	}
	x := v.(getItemResult)
	return x.Map, x.Has, nil
}

type getItemResult struct {
	Map map[string]Value
	Has bool
}

func (d DynamoDB) getItem(mykey string) (out map[string]Value, hasItem bool, err error) {

	out = make(map[string]Value)
	hasItem = true

	now := time.Now()

	transport := http.DefaultTransport

	myid := sType{S: mykey}
	key := keyType{myid}
	content, err := json.Marshal(getRequest{TableName: d.Table, Key: key, ConsistentRead: true})

	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", "https://dynamodb.us-east-1.amazonaws.com/", bytes.NewReader(content))

	if err != nil {
		return
	}

	req.Header.Add("Date", formatTime(now))
	req.Header.Add("X-Amz-Target", "DynamoDB_20111205.GetItem")
	req.Header.Add("Content-Type", "application/x-amz-json-1.0")

	keys := d.keys()

	svc := aws4.Service{Name: "dynamodb", Region: "us-east-1"}

	err = svc.Sign(&keys, req)

	if err != nil {
		return
	}

	resp, err := transport.RoundTrip(req)

	if err != nil {
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		err = errors.New("status " + resp.Status)
		return
	}

	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)

	var v map[string]interface{}

	err = json.Unmarshal(buf.Bytes(), &v)
	if err != nil {
		return
	}

	item, ok := v["Item"].(map[string]interface{})

	if !ok {
		hasItem = false
		return
	}

	for key, value := range item {

		v, ok := value.(map[string]interface{})
		if !ok {
			err = errors.New("bad kv")
			return
		}

		switch {
		case v["N"] != nil:
			n, err2 := strconv.ParseFloat(v["N"].(string), 64)
			if err2 != nil {
				err = err2
				return
			}
			out[key] = Value{Type: N, N: n}
		case v["S"] != nil:
			out[key] = Value{Type: S, S: v["S"].(string)}

		}
	}

	return
}

type deleteRequest struct {
	TableName string
	Key       keyType
}

type putRequest struct {
	TableName string
	Item      map[string]sType
}

type getRequest struct {
	TableName      string
	Key            keyType
	ConsistentRead bool
}

type keyType struct {
	HashKeyElement sType
}

func formatTime(t time.Time) string {
	return t.UTC().Format(http.TimeFormat)
}

func (s DynamoDB) retry(msg string, f func() (interface{}, error)) (v interface{}, err error) {
	return goutil.Retry(msg, s.Strat.NewInstance(), f)
}

func (d DynamoDB) keys() aws4.Keys {
	return aws4.Keys{AccessKey: d.Auth.AccessKey, SecretKey: d.Auth.SecretKey}
}
