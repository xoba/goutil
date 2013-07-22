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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Output struct {
	Collector chan<- KeyValue
	Counters  chan<- Count
}

type Context struct {
	Vars map[string]string `json:",omitempty"`
}

func (o *Output) Close() {
	close(o.Collector)
	close(o.Counters)
}

type MapContext struct {
	Input <-chan KeyValue
	Output
	Context
}
type ReduceContext struct {
	Input <-chan ReduceJob
	Output
	Context
}

type Mapper interface {
	Map(ctx MapContext)
}

type Reducer interface {
	Reduce(ctx ReduceContext)
}

type KeyValue struct {
	Key, Value string
}

type ReduceJob struct {
	Key    string
	Values <-chan string
}
type Count struct {
	Group, Counter string
	Amount         int
}

func RunStreamingMapper(m Mapper) {

	var wg sync.WaitGroup
	defer wg.Wait()

	counters := make(chan Count)
	collector := make(chan KeyValue)

	items := make(chan KeyValue)
	defer close(items)

	wg.Add(1)
	go func() {
		m.Map(MapContext{
			Input: items,
			Output: Output{
				Counters:  counters,
				Collector: collector,
			},
		})
		wg.Done()
	}()

	wg.Add(1)
	go runOutput(&wg, collector)

	wg.Add(1)
	go runCounters(&wg, counters)

	b := bufio.NewReader(os.Stdin)

	for {
		line, err := b.ReadString('\n')
		if err != nil {
			break
		}

		line = line[:len(line)-1]

		items <- ParseLine(line)
	}

}

func ParseLine(line string) KeyValue {
	i := strings.Index(line, "\t")

	if i >= 0 {
		key := line[:i]
		value := line[i+1:]
		return KeyValue{Key: key, Value: value}
	} else {
		return KeyValue{Key: line}
	}
}

func RunStreamingReducer(r Reducer) {

	counters := make(chan Count)
	collector := make(chan KeyValue)

	jobs := make(chan ReduceJob)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		r.Reduce(ReduceContext{
			Input: jobs,
			Output: Output{
				Counters:  counters,
				Collector: collector,
			},
		})
		wg.Done()
	}()

	wg.Add(1)
	go runOutput(&wg, collector)

	wg.Add(1)
	go runCounters(&wg, counters)

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

	if values != nil {
		close(values)
	}

	close(jobs)

	wg.Wait()

}

type Flow struct {
	IsSpot             bool
	Auth               aws.Auth
	Steps              []Step
	Instances          int
	MasterInstanceType string
	MasterSpotPrice    float64
	SlaveInstanceType  string
	SlaveSpotPrice     float64
	ScriptBucket       string
	LogBucket          string
	KeyName            string
}

type Step struct {
	Name            string
	Inputs          []string
	Output          string
	Reducers        int           `json:",omitempty"`
	Timeout         time.Duration `json:",omitempty"`
	Mapper, Reducer tool.Interface
	Compress        bool
	Context
}

func Run(flow Flow) {

	validate(flow)

	id := fmt.Sprintf("%s-%s_%s_%s", flow.Steps[0].Mapper.Name(), flow.Steps[0].Reducer.Name(), time.Now().UTC().Format("20060102T150405Z"), uuid.New()[:4])

	ss3 := s3.GetDefault(flow.Auth)

	v := make(url.Values)

	v.Set("Action", "RunJobFlow")

	v.Set("Name", id)
	v.Set("AmiVersion", "latest")
	v.Set("LogUri", fmt.Sprintf("s3n://%s/%s", flow.LogBucket, id))

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
	} else {
		v.Set("Instances.MasterInstanceType", flow.MasterInstanceType)
		v.Set("Instances.SlaveInstanceType", flow.SlaveInstanceType)
		v.Set("Instances.InstanceCount", fmt.Sprintf("%d", flow.Instances))
	}

	v.Set("Steps.member.1.Name", "debugging")
	v.Set("Steps.member.1.ActionOnFailure", "TERMINATE_JOB_FLOW")
	v.Set("Steps.member.1.HadoopJarStep.Jar", "s3://elasticmapreduce/libs/script-runner/script-runner.jar")
	v.Set("Steps.member.1.HadoopJarStep.Args.member.1", "s3://elasticmapreduce/libs/state-pusher/0.1/fetch")

	for i, step := range flow.Steps {

		n := i + 2

		id := uuid.New()

		mapperObject := s3.Object{Bucket: flow.ScriptBucket, Key: "mapper/" + id}
		reducerObject := s3.Object{Bucket: flow.ScriptBucket, Key: "reducer/" + id}

		check(ss3.PutObject(s3.PutObjectRequest{Object: mapperObject, ContentType: "application/octet-stream", Data: []byte(createScript(step.Mapper))}))
		check(ss3.PutObject(s3.PutObjectRequest{Object: reducerObject, ContentType: "application/octet-stream", Data: []byte(createScript(step.Reducer))}))

		{
			v.Set(fmt.Sprintf("Steps.member.%d.Name", n), step.Name)
			v.Set(fmt.Sprintf("Steps.member.%d.ActionOnFailure", n), "TERMINATE_JOB_FLOW")
			v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Jar", n), "/home/hadoop/contrib/streaming/hadoop-streaming.jar")

			i := func() func() int {
				i := 0
				return func() int {
					i++
					return i
				}
			}()

			if step.Reducers > 0 {
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "-D")
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), fmt.Sprintf("mapred.reduce.tasks=%d", step.Reducers))
			}

			if step.Timeout > 0 {
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "-D")
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), fmt.Sprintf("mapred.task.timeout=%d", step.Timeout.Nanoseconds()/1000000))

			}

			if step.Compress {
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "-jobconf")
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "mapred.output.compress=true")
			}

			for _, s := range step.Inputs {
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "-input")
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), s)
			}

			v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "-output")
			v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), step.Output)
			v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "-mapper")
			v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), toUrl(mapperObject))
			v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "-reducer")
			v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), toUrl(reducerObject))

			for k, x := range step.Vars {
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), "-cmdenv")
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), fmt.Sprintf("%s=%s", k, x))
			}
		}

	}

	{
		var keys []string
		for k, _ := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("%s: %s\n", k, v.Get(k))
		}
	}

	u := createSignedURL(flow.Auth, v)

	fmt.Print(string(debugReq(u)))

}

func runOutput(wg *sync.WaitGroup, collector chan KeyValue) {
	defer wg.Done()
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	for kv := range collector {
		out.WriteString(fmt.Sprintf("%s\t%s\n", kv.Key, kv.Value))
	}
}

func runCounters(wg *sync.WaitGroup, counters chan Count) {
	defer wg.Done()
	for c := range counters {
		c.Group = strings.Replace(c.Group, ",", "", -1)
		c.Counter = strings.Replace(c.Counter, ",", "", -1)
		if len(c.Group) == 0 {
			c.Group = "global"
		}
		if len(c.Counter) == 0 {
			c.Counter = "job"
		}
		os.Stderr.Write([]byte(fmt.Sprintf("reporter:counter:%s,%s,%d\n", c.Group, c.Counter, c.Amount)))
	}
}

func isNull(x string, s string) {
	if len(s) == 0 {
		panic("zero-length string: " + x)
	}
}

func validate(f Flow) {

	for _, s := range f.Steps {
		for _, i := range s.Inputs {
			isNull("Input", i)
		}
		isNull("Output", s.Output)
		if strings.Contains(s.Name, " ") {
			panic("step name can't contain spaces")
		}
	}

	isNull("MasterInstanceType", f.MasterInstanceType)
	isNull("SlaveInstanceType", f.SlaveInstanceType)
	isNull("ScriptBucket", f.ScriptBucket)
	isNull("LogBucket", f.LogBucket)
	isNull("KeyName", f.KeyName)
}

func createSignedURL(auth aws.Auth, v url.Values) string {

	v.Set("Version", "2009-03-31")
	v.Set("SignatureVersion", "2")
	v.Set("SignatureMethod", "HmacSHA256")
	v.Set("AWSAccessKeyId", auth.AccessKey)
	v.Set("Timestamp", iso(time.Now()))

	var lines []string

	add := func(s string) {
		lines = append(lines, s)
	}

	add("GET")
	add("elasticmapreduce.amazonaws.com")
	add("/")
	add(v.Encode())

	str := strings.Join(lines, "\n")

	v.Set("Signature", ghmac(auth.SecretKey, str))

	return fmt.Sprintf("https://elasticmapreduce.amazonaws.com?%s", v.Encode())

}

func debugReq(u string) []byte {
	resp, err := http.Get(u)
	check(err)
	defer resp.Body.Close()

	if resp.Status != "200 OK" {
		fmt.Printf("status: %s\n", resp.Status)
		fmt.Printf("header: %v\n", resp.Header)
	}

	var buf bytes.Buffer
	io.Copy(&buf, resp.Body)
	return buf.Bytes()
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

func NewReduceTool(r Reducer, name, description string) *ReduceTool {
	return &ReduceTool{
		reducer:     r,
		name:        name,
		description: description,
	}
}
func NewHiddenReduceTool(r Reducer, name, description string) *ReduceTool {
	return &ReduceTool{
		reducer:     r,
		name:        name,
		description: description,
		hidden:      true,
	}
}

func NewMapTool(m Mapper, name, description string) *MapTool {
	return &MapTool{
		mapper:      m,
		name:        name,
		description: description,
	}
}
func NewHiddenMapTool(m Mapper, name, description string) *MapTool {
	return &MapTool{
		mapper:      m,
		name:        name,
		description: description,
		hidden:      true,
	}
}

type MapTool struct {
	mapper      Mapper
	name        string
	description string
	hidden      bool
}

func (t *MapTool) MarshalJSON() ([]byte, error) {
	return marshal(t)
}
func (t *MapTool) Tags() []string {
	if t.hidden {
		return []string{"hidden"}
	} else {
		return []string{}
	}
}
func (m *MapTool) Name() string {
	return m.name
}
func (m *MapTool) String() string {
	return m.Name()
}
func (m *MapTool) Description() string {
	return m.description
}

func (m *MapTool) Run(args []string) {
	RunStreamingMapper(m.mapper)
}

type ReduceTool struct {
	reducer     Reducer
	name        string
	description string
	hidden      bool
}

func (t *ReduceTool) MarshalJSON() ([]byte, error) {
	return marshal(t)
}
func (t *ReduceTool) Tags() []string {
	if t.hidden {
		return []string{"hidden"}
	} else {
		return []string{}
	}
}
func (m *ReduceTool) Name() string {
	return m.name
}
func (m *ReduceTool) String() string {
	return m.Name()
}
func (m *ReduceTool) Description() string {
	return m.description
}

func (m *ReduceTool) Run(args []string) {
	RunStreamingReducer(m.reducer)
}

func marshal(i fmt.Stringer) ([]byte, error) {
	return []byte(fmt.Sprintf(`{"type":"%T"}`, i)), nil
}

type IdentityMapperTool struct {
	Id      string
	Taglist []string
}

func (t *IdentityMapperTool) MarshalJSON() ([]byte, error) {
	return marshal(t)
}

func (t *IdentityMapperTool) Tags() []string {
	return t.Taglist
}
func (m *IdentityMapperTool) String() string {
	return m.Name()
}
func (m *IdentityMapperTool) Name() string {
	return m.Id
}
func (m *IdentityMapperTool) Description() string {
	return "identity mapper"
}
func (m *IdentityMapperTool) Run(args []string) {
	bufferedCopy()
}

type IdentityMapper struct {
}

func (m *IdentityMapper) Map(ctx MapContext) {
	defer ctx.Close()
	for kv := range ctx.Input {
		ctx.Collector <- kv
	}
}

type IdentityReducer struct {
}

func (s *IdentityReducer) Reduce(ctx ReduceContext) {
	defer ctx.Close()
	for j := range ctx.Input {
		for v := range j.Values {
			ctx.Collector <- KeyValue{j.Key, v}
		}
	}
}

type IdentityReducerTool struct {
	Id      string
	Taglist []string
}

func (t *IdentityReducerTool) MarshalJSON() ([]byte, error) {
	return marshal(t)
}

func (t *IdentityReducerTool) Tags() []string {
	return t.Taglist
}
func (m *IdentityReducerTool) String() string {
	return m.Name()
}
func (m *IdentityReducerTool) Name() string {
	return m.Id
}
func (m *IdentityReducerTool) Description() string {
	return "identity reducer"
}
func (m *IdentityReducerTool) Run(args []string) {
	bufferedCopy()
}

func bufferedCopy() {
	r := bufio.NewReader(os.Stdin)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	io.Copy(w, r)
}

type IntegerSumReducer struct {
}

func (s *IntegerSumReducer) Reduce(ctx ReduceContext) {
	defer ctx.Close()

	m := make(map[string]int)

	for j := range ctx.Input {
		for v := range j.Values {
			i, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				m[j.Key] += int(i)
			}
		}
	}

	for k, v := range m {
		ctx.Collector <- KeyValue{Key: k, Value: fmt.Sprintf("%d", v)}
	}
}

func tick(ch chan<- Count, count int) {
	ch <- Count{Group: "ticks", Counter: fmt.Sprintf("minutes-%03d", count), Amount: 1}
}

func TicksDone(ch chan<- Count) {
	ch <- Count{Group: "ticks", Counter: "done", Amount: 1}
}

func StartTicker(ch chan<- Count) {
	tick(ch, 0)
	go func() {
		var minutes int
		c := time.Tick(1 * time.Minute)
		for _ = range c {
			minutes++
			tick(ch, minutes)
		}
	}()
}
