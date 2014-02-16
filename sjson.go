package goutil

// simple json handling, inspired by https://github.com/bitly/go-simplejson

import (
	"encoding/json"
	"errors"
	"log"
)

type Json struct {
	data interface{}
}

func WrapJson(i interface{}) *Json {
	return &Json{i}
}

func NewJson(b []byte) (*Json, error) {
	j := new(Json)
	if err := j.UnmarshalJSON(b); err != nil {
		return nil, err
	} else {
		return j, nil
	}
}

func (j *Json) AsMap() (map[string]interface{}, error) {
	if m, ok := (j.data).(map[string]interface{}); ok {
		return m, nil
	}
	return nil, errors.New("type assertion to map[string]interface{} failed")
}

func (j *Json) Get(key string) *Json {
	m, err := j.AsMap()
	if err == nil {
		if val, ok := m[key]; ok {
			return &Json{val}
		}
	}
	return &Json{nil}
}

func (j *Json) GetPath(branch ...string) *Json {
	jin := j
	for i := range branch {
		m, err := jin.AsMap()
		if err != nil {
			return &Json{nil}
		}
		if val, ok := m[branch[i]]; ok {
			jin = &Json{val}
		} else {
			return &Json{nil}
		}
	}
	return jin
}

func (j *Json) Set(key string, val interface{}) {
	m, err := j.AsMap()
	if err != nil {
		return
	}
	m[key] = val
}

func (j *Json) GetIndex(index int) *Json {
	a, err := j.AsArray()
	if err == nil {
		if len(a) > index {
			return &Json{a[index]}
		}
	}
	return &Json{nil}
}

func (j *Json) AsArray() ([]interface{}, error) {
	if a, ok := (j.data).([]interface{}); ok {
		return a, nil
	}
	return nil, errors.New("type assertion to []interface{} failed")
}

func (j *Json) StringArray() ([]string, error) {
	arr, err := j.AsArray()
	if err != nil {
		return nil, err
	}
	retArr := make([]string, 0, len(arr))
	for _, a := range arr {
		s, ok := a.(string)
		if !ok {
			return nil, err
		}
		retArr = append(retArr, s)
	}
	return retArr, nil
}

func (j *Json) AsBool() (bool, error) {
	if s, ok := (j.data).(bool); ok {
		return s, nil
	}
	return false, errors.New("type assertion to bool failed")
}

func (j *Json) AsString() (string, error) {
	if s, ok := (j.data).(string); ok {
		return s, nil
	}
	return "", errors.New("type assertion to string failed")
}

func (j *Json) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.data)
}

func (j *Json) UnmarshalJSON(b []byte) error {
	var m interface{}
	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}
	j.data = m
	return nil
}

func (j *Json) String() string {
	buf, err := json.Marshal(j.data)
	if err != nil {
		panic(err)
	}
	return string(buf)
}

type TestJson struct {
}

func (TestJson) Name() string {
	return "sjson,test simple json lib"
}
func (TestJson) Run(args []string) {
	test1()
	testmap()
	testarray()
	testbool()
	teststring()
	testget()
}
func testget() {
	js, err := NewJson([]byte(`{ 
		"test": { 
			"array": [1, "2", 3],
			"arraywithsubs": [
				{"subkeyone": 1},
				{"subkeytwo": 2, "subkeythree": 3}
			],
			"bignum": 9223372036854775807
		}
	}`))
	if err != nil {
		log.Fatal(err)
	}
	x := js.Get("test").Get("bignum")
	log.Printf("get: %#v\n", x)
	log.Printf("getpath: %#v\n", js.GetPath("test", "array"))
}

func teststring() {
	js, err := NewJson([]byte(`"xyz"`))
	if err != nil {
		log.Fatal(err)
	}
	b, err := js.AsString()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("string: %#v\n", b)
}

func testbool() {
	js, err := NewJson([]byte(`true`))
	if err != nil {
		log.Fatal(err)
	}
	b, err := js.AsBool()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("bool: %#v\n", b)
}

func testarray() {
	js, err := NewJson([]byte(`["abc",123]`))
	if err != nil {
		log.Fatal(err)
	}
	a, err := js.AsArray()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("array: %#v\n", a)
}

func testmap() {
	js, err := NewJson([]byte(`{"abc":123}`))
	if err != nil {
		log.Fatal(err)
	}
	m, err := js.AsMap()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("map: %#v\n", m)
}

func test1() {
	js, err := NewJson([]byte(`[3.14,123]`))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("json: %#v\n", js)
}
