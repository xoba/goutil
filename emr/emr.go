package emr

import (
	"bufio"
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/xoba/goutil/aws"
	"github.com/xoba/goutil/aws/s3"
	"github.com/xoba/goutil/tool"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type Reducer interface {
	Reduce(jobs <-chan ReduceJob, collector chan<- KeyValue)
}
type KeyValue struct {
	Key, Value string
}
type ReduceJob struct {
	Key    string
	Values <-chan string
}

func RunStreamingReducer(r Reducer) {

	collector := make(chan KeyValue)
	jobs := make(chan ReduceJob)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		r.Reduce(jobs, collector)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		func() {
			out := bufio.NewWriter(os.Stdout)
			defer out.Flush()
			for kv := range collector {
				out.WriteString(fmt.Sprintf("%s\t%s\n", kv.Key, kv.Value))
			}
		}()
		wg.Done()
	}()

	b := bufio.NewReader(os.Stdin)

	var lastKey *string
	var values chan string

	for {
		line, err := b.ReadString('\n')
		if err != nil {
			break
		}

		line = line[:len(line)-1]

		i := strings.Index(line, "\t")

		if i >= 0 {

			key := line[:i]
			value := line[i+1:]

			if lastKey == nil || *lastKey != key {
				lastKey = &key
				if values != nil {
					close(values)
				}
				values = make(chan string)
				jobs <- ReduceJob{
					Key:    key,
					Values: values,
				}
			}

			values <- value
		}

	}

	close(values)
	close(jobs)

	wg.Wait()

}

type Flow struct {
	IsSpot             bool
	Auth               aws.Auth
	Mapper, Reducer    tool.Interface
	Input, Output      string
	Instances          int
	MasterInstanceType string
	MasterSpotPrice    float64
	SlaveInstanceType  string
	SlaveSpotPrice     float64
	ScriptBucket       string
	LogBucket          string
	KeyName            string
}

func isNull(x string, s string) {
	if len(s) == 0 {
		panic("zero-length string: " + x)
	}
}

func validate(f Flow) {
	isNull("Input", f.Input)
	isNull("Output", f.Output)
	isNull("MasterInstanceType", f.MasterInstanceType)
	isNull("SlaveInstanceType", f.SlaveInstanceType)
	isNull("ScriptBucket", f.ScriptBucket)
	isNull("LogBucket", f.LogBucket)
	isNull("KeyName", f.KeyName)
}

func Run(flow Flow) {

	validate(flow)

	id := fmt.Sprintf("%s-%s_%s_%s", flow.Mapper.Name(), flow.Reducer.Name(), time.Now().UTC().Format("20060102T150405Z"), uuid.New()[:4])

	ss3 := s3.GetDefault(flow.Auth)

	mapperObject := s3.Object{Bucket: flow.ScriptBucket, Key: "mapper/" + id}
	reducerObject := s3.Object{Bucket: flow.ScriptBucket, Key: "reducer/" + id}

	check(ss3.PutObject(s3.PutObjectRequest{Object: mapperObject, ContentType: "application/octet-stream", Data: []byte(createScript(flow.Mapper))}))
	check(ss3.PutObject(s3.PutObjectRequest{Object: reducerObject, ContentType: "application/octet-stream", Data: []byte(createScript(flow.Reducer))}))

	v := make(url.Values)

	v.Set("Action", "RunJobFlow")

	v.Set("Name", id)
	v.Set("AmiVersion", "latest")
	v.Set("LogUri", fmt.Sprintf("s3n://%s/%s", flow.LogBucket, id))

	if !flow.IsSpot {
		v.Set("Instances.MasterInstanceType", flow.MasterInstanceType)
		v.Set("Instances.SlaveInstanceType", flow.SlaveInstanceType)
		v.Set("Instances.InstanceCount", fmt.Sprintf("%d", flow.Instances))
	}

	v.Set("Instances.Ec2KeyName", flow.KeyName)
	v.Set("Instances.Placement.AvailabilityZone", "us-east-1d")
	v.Set("Instances.KeepJobFlowAliveWhenNoSteps", "false")
	v.Set("Instances.TerminationProtected", "false")

	if flow.IsSpot {
		v.Set("Instances.InstanceGroups.member.1.InstanceRole", "MASTER")
		v.Set("Instances.InstanceGroups.member.1.Market", "SPOT")
		v.Set("Instances.InstanceGroups.member.1.BidPrice", fmt.Sprintf("%.3f", flow.MasterSpotPrice))
		v.Set("Instances.InstanceGroups.member.1.InstanceType", flow.MasterInstanceType)
		v.Set("Instances.InstanceGroups.member.1.InstanceCount", "1")

		v.Set("Instances.InstanceGroups.member.2.InstanceRole", "CORE")
		v.Set("Instances.InstanceGroups.member.2.Market", "SPOT")
		v.Set("Instances.InstanceGroups.member.2.BidPrice", fmt.Sprintf("%.3f", flow.SlaveSpotPrice))
		v.Set("Instances.InstanceGroups.member.2.InstanceType", flow.SlaveInstanceType)
		v.Set("Instances.InstanceGroups.member.2.InstanceCount", fmt.Sprintf("%d", flow.Instances))
	}

	v.Set("Steps.member.1.Name", "debugging")
	v.Set("Steps.member.1.ActionOnFailure", "TERMINATE_JOB_FLOW")
	v.Set("Steps.member.1.HadoopJarStep.Jar", "s3://elasticmapreduce/libs/script-runner/script-runner.jar")
	v.Set("Steps.member.1.HadoopJarStep.Args.member.1", "s3://elasticmapreduce/libs/state-pusher/0.1/fetch")

	v.Set("Steps.member.2.Name", "streaming")
	v.Set("Steps.member.2.ActionOnFailure", "TERMINATE_JOB_FLOW")
	v.Set("Steps.member.2.HadoopJarStep.Jar", "/home/hadoop/contrib/streaming/hadoop-streaming.jar")
	v.Set("Steps.member.2.HadoopJarStep.Args.member.1", "-input")
	v.Set("Steps.member.2.HadoopJarStep.Args.member.2", flow.Input)
	v.Set("Steps.member.2.HadoopJarStep.Args.member.3", "-output")
	v.Set("Steps.member.2.HadoopJarStep.Args.member.4", flow.Output)
	v.Set("Steps.member.2.HadoopJarStep.Args.member.5", "-mapper")
	v.Set("Steps.member.2.HadoopJarStep.Args.member.6", toUrl(mapperObject))
	v.Set("Steps.member.2.HadoopJarStep.Args.member.7", "-reducer")
	v.Set("Steps.member.2.HadoopJarStep.Args.member.8", toUrl(reducerObject))

	for k, _ := range v {
		fmt.Printf("%50s: %s\n", k, v.Get(k))
	}

	u := createSignedURL(flow.Auth, v)

	debugReq(u)

}

func createSignedURL(auth aws.Auth, v url.Values) string {

	v.Set("Version", "2009-03-31")
	v.Set("SignatureVersion", "2")
	v.Set("SignatureMethod", "HmacSHA256")
	v.Set("AWSAccessKeyId", auth.AccessKey)
	v.Set("Timestamp", iso(time.Now()))

	var lines []string

	a := func(s string) {
		lines = append(lines, s)
	}

	a("GET")
	a("elasticmapreduce.amazonaws.com")
	a("/")
	a(v.Encode())

	str := strings.Join(lines, "\n")

	v.Set("Signature", ghmac(auth.SecretKey, str))

	return fmt.Sprintf("https://elasticmapreduce.amazonaws.com?%s", v.Encode())

}

func debugReq(u string) {
	fmt.Printf("api call --- %s\n", u)

	resp, err := http.Get(u)
	check(err)
	defer resp.Body.Close()

	fmt.Printf("status: %s\n", resp.Status)
	fmt.Printf("header: %v\n", resp.Header)

	io.Copy(os.Stdout, resp.Body)

}

const (
	ISO = "2006-01-02T15:04:05"
)

func iso(t time.Time) string {
	return t.UTC().Format(ISO)
}

func ghmac(key, data string) string {
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(data))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func split(s string) string {

	var buf bytes.Buffer

	w := func(s string) {
		buf.Write([]byte(s))
		buf.Write([]byte("\n"))
	}

	for {
		if len(s) > 100 {
			w(s[:100])
			s = s[100:]
		} else {
			w(s)
			break
		}
	}
	return string(buf.Bytes())
}

func createScript(t tool.Interface) string {
	cmd := "go/bin/" + os.Args[0]

	buf, err := ioutil.ReadFile(cmd)
	check(err)

	{
		var g bytes.Buffer
		gz := gzip.NewWriter(&g)
		gz.Write(buf)
		gz.Close()
		buf = g.Bytes()
	}

	var f bytes.Buffer
	w := func(s string) {
		f.Write([]byte(s))
		f.Write([]byte("\n"))
	}
	w2 := func(s string) {
		f.Write([]byte(s))
	}

	w("#!/bin/bash")
	w(fmt.Sprintf("CMD=/tmp/`/bin/mktemp -u %s_XXXXXXXXXXXXX`", t.Name()))
	w("/usr/bin/base64 -d <<END_TEXT | /bin/gzip -d > $CMD")
	w2(split(base64.StdEncoding.EncodeToString(buf)))
	w("END_TEXT")
	w("/bin/chmod 777 $CMD")
	w(fmt.Sprintf("$CMD %s", t.Name()))
	w("/bin/rm $CMD")

	return string(f.Bytes())
}

func toUrl(o s3.Object) string {
	return fmt.Sprintf("s3n://%s/%s", o.Bucket, o.Key)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
