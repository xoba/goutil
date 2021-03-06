// code for elastic mapreduce (streaming).
package emr

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/xoba/goutil"
	"github.com/xoba/goutil/aws"
	"github.com/xoba/goutil/aws/s3"
	"github.com/xoba/goutil/tool"
)

type Mapper func(ctx MapContext)
type Reducer func(ctx ReduceContext)

type MapContext struct {
	Input <-chan KeyValue
	Output
	Context
	Error
}

type Error struct {
	Error error
}

type ReduceContext struct {
	Input <-chan ReduceJob
	Output
	Context
}

type KeyValue struct {
	Key, Value string
}

type ReduceJob struct {
	Key    string
	Values <-chan string
}

// mappers and reducers should not close these channels!
type Output struct {
	Collector chan<- KeyValue
	Counters  chan<- Count
}

func (o *Output) close() {
	close(o.Collector)
	close(o.Counters)
}

type Context struct {
	Vars     map[string]string
	Filename string
}

type Count struct {
	Group, Counter string
	Amount         int
}

func grepContext(fn string) Context {

	if len(fn) == 0 {
		fn = os.Getenv("map_input_file")
	}

	out := Context{Filename: fn, Vars: make(map[string]string)}

	// grep special vars from environment
	for _, x := range os.Environ() {
		parts := strings.Split(x, "=")
		if len(parts) == 2 {
			key := parts[0]
			if strings.HasPrefix(key, VARS_PREFIX) {
				out.Vars[key[len(VARS_PREFIX):]] = parts[1]
			}
		}
	}
	return out
}

func runStreamingMapper(r io.Reader, ctx Context, m Mapper) {

	var wg sync.WaitGroup
	defer wg.Wait()

	counters := make(chan Count)
	collector := make(chan KeyValue)

	items := make(chan KeyValue)

	wg.Add(1)
	go func() {
		defer wg.Done()

		output := Output{
			Counters:  counters,
			Collector: collector,
		}

		defer output.close()

		m(MapContext{
			Input:   items,
			Output:  output,
			Context: ctx,
		})
	}()

	wg.Add(1)
	go runOutput(&wg, collector)

	wg.Add(1)
	go runCounters(&wg, counters)

	err := SlurpLines(r, func(line string) {
		items <- ParseLine(line)
	})
	close(items)

	if err != nil {
		// somehow, handle error
	}
}

func runStreamingReducer(r Reducer) {

	counters := make(chan Count)
	collector := make(chan KeyValue)

	jobs := make(chan ReduceJob)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		output := Output{
			Counters:  counters,
			Collector: collector,
		}

		defer output.close()

		r(ReduceContext{
			Input: jobs,
			Output: Output{
				Counters:  counters,
				Collector: collector,
			},
			Context: grepContext(""),
		})
	}()

	wg.Add(1)
	go runOutput(&wg, collector)

	wg.Add(1)
	go runCounters(&wg, counters)

	var lastKey *string
	var values chan string

	procLine := func(line string) {
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

	err := SlurpLines(os.Stdin, procLine)
	if err != nil {
		os.Exit(1)
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
	MasterSpotPrice    float64 `json:",omitempty"`
	SlaveInstanceType  string
	SlaveSpotPrice     float64 `json:",omitempty"`
	ScriptBucket       string
	LogBucket          string
	KeepAlive          bool
	KeyName            string
	AvailabilityZone   string
}

type Step struct {
	Name               string
	Inputs             []string
	Output             string
	Reducers           int           `json:",omitempty"`
	Timeout            time.Duration `json:",omitempty"`
	Mapper, Reducer    Streaming
	Compress           bool              `json:",omitempty"`
	CompressMapOutput  bool              `json:",omitempty"`
	SortSecondKeyField bool              `json:",omitempty"`
	ToolChecker        ToolChecker       `json:",omitempty"`
	Vars               map[string]string `json:",omitempty"`

	// additional args on streaming command
	Args []string `json:",omitempty"`

	// this is a big one: determines whether input files are lists of url's or not
	IndirectMapJob bool `json:",omitempty"`
}

func Run(flow Flow) (*RunFlowResponse, error) {

	validate(flow)

	if !flow.IsSpot {
		flow.MasterSpotPrice = 0
		flow.SlaveSpotPrice = 0
	}

	id := fmt.Sprintf("%s-%s_%s_%s", tool.Name(flow.Steps[0].Mapper), tool.Name(flow.Steps[0].Reducer), time.Now().UTC().Format("20060102T150405Z"), uuid.New()[:4])

	ss3 := s3.GetDefault(flow.Auth)

	v := make(url.Values)

	v.Set("Action", "RunJobFlow")

	v.Set("Name", id)
	v.Set("AmiVersion", "2.4.3")
	v.Set("LogUri", fmt.Sprintf("s3n://%s/%s", flow.LogBucket, id))

	v.Set("Instances.Ec2KeyName", flow.KeyName)
	v.Set("Instances.HadoopVersion", "1.0.3")

	if len(flow.AvailabilityZone) == 0 {
		flow.AvailabilityZone = "us-east-1d"
	}

	v.Set("Instances.Placement.AvailabilityZone", flow.AvailabilityZone)
	v.Set("Instances.KeepJobFlowAliveWhenNoSteps", fmt.Sprintf("%v", flow.KeepAlive))
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
		v.Set("Instances.InstanceGroups.member.2.InstanceCount", fmt.Sprintf("%d", flow.Instances-1))
	} else {
		v.Set("Instances.MasterInstanceType", flow.MasterInstanceType)
		v.Set("Instances.SlaveInstanceType", flow.SlaveInstanceType)
		v.Set("Instances.InstanceCount", fmt.Sprintf("%d", flow.Instances))
	}

	failureAction := func() string {
		if flow.KeepAlive {
			return "CANCEL_AND_WAIT"
		} else {
			return "TERMINATE_JOB_FLOW"
		}
	}()

	v.Set("Steps.member.1.Name", "debugging")
	v.Set("Steps.member.1.ActionOnFailure", failureAction)
	v.Set("Steps.member.1.HadoopJarStep.Jar", "s3://elasticmapreduce/libs/script-runner/script-runner.jar")
	v.Set("Steps.member.1.HadoopJarStep.Args.member.1", "s3://elasticmapreduce/libs/state-pusher/0.1/fetch")

	for i, step := range flow.Steps {

		n := i + 2

		id := uuid.New()

		mapperObject := s3.Object{Bucket: flow.ScriptBucket, Key: "mapper/" + id}
		reducerObject := s3.Object{Bucket: flow.ScriptBucket, Key: "reducer/" + id}

		{
			var args []string
			args = append(args, fmt.Sprintf("-indirect=%v", step.IndirectMapJob))
			args = append(args, step.Args...)
			check(ss3.PutObject(s3.PutObjectRequest{BasePut: s3.BasePut{Object: mapperObject, ContentType: "application/octet-stream"}, Data: []byte(createScript(step.Mapper, step.ToolChecker, args...))}))
		}

		{
			var args []string
			args = append(args, step.Args...)
			check(ss3.PutObject(s3.PutObjectRequest{BasePut: s3.BasePut{Object: reducerObject, ContentType: "application/octet-stream"}, Data: []byte(createScript(step.Reducer, step.ToolChecker, args...))}))
		}

		{
			v.Set(fmt.Sprintf("Steps.member.%d.Name", n), step.Name)
			v.Set(fmt.Sprintf("Steps.member.%d.ActionOnFailure", n), failureAction)
			v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Jar", n), "/home/hadoop/contrib/streaming/hadoop-streaming.jar")

			i := func() func() int {
				i := 0
				return func() int {
					i++
					return i
				}
			}()

			arg := func(a string) {
				v.Set(fmt.Sprintf("Steps.member.%d.HadoopJarStep.Args.member.%d", n, i()), a)
			}

			pair := func(a, b string) {
				arg(a)
				arg(b)
			}

			if step.CompressMapOutput {
				pair("-D", "mapred.compress.map.output=true")
			}

			if step.Reducers > 0 {
				pair("-D", fmt.Sprintf("mapred.reduce.tasks=%d", step.Reducers))
			}

			if step.Timeout > 0 {
				pair("-D", fmt.Sprintf("mapred.task.timeout=%d", step.Timeout.Nanoseconds()/1000000))

			}

			if step.SortSecondKeyField {
				pair("-D", "stream.num.map.output.key.fields=2")
				pair("-D", "mapred.text.key.partitioner.options=-k1,1")
				pair("-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner")
			}

			if step.Compress {
				pair("-jobconf", "mapred.output.compress=true")
			}

			for _, s := range step.Inputs {
				pair("-input", s)
			}

			pair("-output", step.Output)
			pair("-mapper", toUrl(mapperObject))
			pair("-reducer", toUrl(reducerObject))

			for k, x := range step.Vars {
				pair("-cmdenv", fmt.Sprintf("%s%s=%s", VARS_PREFIX, k, x))
			}
		}

	}

	{
		var keys []string
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("%s: %s\n", k, v.Get(k))
		}
	}

	u := createSignedURL(flow.Auth, v)

	resp, err := runReq(u)
	if err != nil {
		return nil, err
	}
	return ParseEmrResponse(resp)
}

func runReq(u string) (io.ReadCloser, error) {
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, fmt.Errorf("bad response: %s", resp.Status)
	}
	return resp.Body, nil
}

const VARS_PREFIX = "EMR_VARS_"

func runOutput(wg *sync.WaitGroup, collector chan KeyValue) {
	defer wg.Done()
	for kv := range collector {
		fmt.Fprintf(os.Stdout, "%s\t%s\n", kv.Key, kv.Value)
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
		count(c.Group, c.Counter, c.Amount)
	}
}

func AlphaNumFilter(s string) string {
	out := new(bytes.Buffer)
	for _, x := range s {
		switch {
		case (x >= '0' && x <= '9') || (x >= 'A' && x <= 'Z') || (x >= 'a' && x <= 'z'):
			fallthrough
		case x == '(':
			fallthrough
		case x == ')':
			fallthrough
		case x == '[':
			fallthrough
		case x == ']':
			fallthrough
		case x == '*':
			fallthrough
		case x == ' ':
			out.WriteRune(x)
		default:
			out.WriteRune('_')
		}
	}
	s = strings.Replace(s, "\n", "_", -1)
	return string(out.Bytes())
}

func count(a, b string, amount int) {
	fmt.Fprintf(os.Stderr, "reporter:counter:%s,%s,%d\n", AlphaNumFilter(a), AlphaNumFilter(b), amount)
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
	add(encode(v))

	str := strings.Join(lines, "\n")

	v.Set("Signature", ghmac(auth.SecretKey, str))

	return fmt.Sprintf("https://elasticmapreduce.amazonaws.com?%s", encode(v))

}

// aws requires "space" to be mapped to "%20", not "+" like go's encoder.
func encode(v url.Values) string {
	e := v.Encode()
	e = strings.Replace(e, "+", "%20", -1)
	return e
}

func iso(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05")
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

// returns false if somehow tool doesn't check out
type ToolChecker func(path string, t tool.Interface) error

func (t *ToolChecker) MarshalJSON() ([]byte, error) {
	return []byte(`"[ToolChecker]"`), nil
}

func NullChecker(path string, t tool.Interface) error {
	return nil
}

func LapackToolChecker(path string, t tool.Interface) error {
	var buf bytes.Buffer
	cmd := exec.Command("ldd", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Start()
	done := make(chan error)
	go func() {
		_, err := io.Copy(&buf, stdout)
		done <- err
	}()
	if err := <-done; err != nil {
		return err
	}
	err = cmd.Wait()
	if err != nil {
		return err
	}
	expr := "(?i)(lapack|fortran|blas)"
	r := regexp.MustCompile(expr)
	if r.MatchString(string(buf.Bytes())) {
		return errors.New(fmt.Sprintf("output of \"ldd %s\" matches %s", path, expr))
	}
	return nil
}

func createScript(t tool.Interface, checker ToolChecker, args ...string) string {

	cmd := "bin/" + os.Args[0]

	if checker == nil {
		// since amazon emr doesn't have these libs
		checker = LapackToolChecker
	}
	if err := checker(cmd, t); err != nil {
		panic(fmt.Sprintf("tool %s doesn't check out: %v", tool.Name(t), err))
	}

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
	w(fmt.Sprintf("CMD=/tmp/`/bin/mktemp -u %s_XXXXXXXXXXXXX`", tool.Name(t)))
	w("/usr/bin/base64 -d <<END_TEXT | /bin/gzip -d > $CMD")
	w2(split(base64.StdEncoding.EncodeToString(buf)))
	w("END_TEXT")
	w("/bin/chmod 777 $CMD")

	run := func() string {
		f := new(bytes.Buffer)
		fmt.Fprintf(f, "$CMD %s", tool.Name(t))
		for _, a := range args {
			fmt.Fprintf(f, " %s", a)
		}
		return string(f.Bytes())
	}()

	w(run)

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

// streaming interface is basically just tool.Interface, but with a private method just to make sure
// that nobody outside of this package can implement it! i.e., we control which kinds of tools can
// be mappers and reducers
type Streaming interface {
	streamingMarker
	tool.Interface
}
type streamingMarker interface {
	// method does nothing, it's just a marker
	emrMarker()
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

func runTicker(name string, d time.Duration) {
	var c int
	for {
		time.Sleep(d)
		count(name, fmt.Sprintf("%03d (%s)", c, goutil.FormatDuration(d)), 1)
		c++
		d *= 4
		d /= 3
	}
}

func (m *MapTool) Run(args []string) {

	var indirect bool
	flags := flag.NewFlagSet(m.Name(), flag.ExitOnError)
	flags.BoolVar(&indirect, "indirect", false, "whether input files are indirect")
	flags.Parse(args)

	go runTicker("map", TICKER)

	if indirect {

		urls, err := func() ([]string, error) {
			var out []string
			d := json.NewDecoder(os.Stdin)
			for {
				line := make(map[string]string)
				err := d.Decode(&line)
				if err != nil {
					return out, err
				}
				if u, ok := line["url"]; ok && len(u) > 0 {
					out = append(out, u)
				}
			}
		}()

		count("indirect", "files.found", len(urls))

		if err != io.EOF {
			fmt.Printf("error1; %s; %v\t1\n", os.Getenv("map_input_file"), err)
		}

		for _, u := range urls {

			func() {
				count("indirect", "files.started", 1)
				defer count("indirect", "files.ended", 1)

				r, err := StreamUrl(u, 5, 1*time.Second)

				if err == nil {
					defer r.Close()

					counter := &counter{r: r}

					defer func() {
						count("indirect", "bytes", counter.GetBytes())
					}()

					runStreamingMapper(counter, grepContext(u), m.mapper)

				} else {
					count("indirect", "files.error2", 1)
					// handle file open error
					fmt.Printf("error2 %s; %s; %v\t1\n", os.Getenv("map_input_file"), u, err)
				}
			}()

		}

	} else {
		runStreamingMapper(os.Stdin, grepContext(""), m.mapper)
	}
}

type counter struct {
	r     io.Reader
	count int
}

func (c *counter) GetBytes() int {
	return c.count
}

func (c *counter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.count += n
	return n, err
}

func UrlHasExtension(u, ext string) bool {
	x, err := url.Parse(u)
	if err != nil {
		return false
	}
	return strings.HasSuffix(x.Path, ext)
}

func StreamUrl(u string, max int, backoff time.Duration) (io.ReadCloser, error) {
	return streamUrl(u, 0, max, backoff)
}

func streamUrl(u string, attempt, max int, backoff time.Duration) (io.ReadCloser, error) {

	retry := func() (io.ReadCloser, error) {
		if attempt < max {
			time.Sleep(backoff)
			return streamUrl(u, attempt+1, max, 2*backoff)
		} else {
			return nil, fmt.Errorf("max attempts failed for %s", u)
		}
	}

	tr := &http.Transport{
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}
	resp, err := client.Get(u)
	if err != nil {
		return retry()
	}

	r := resp.Body

	bz2 := resp.Header.Get("Content-Encoding") == "bzip2" || UrlHasExtension(u, ".bz2")
	gz := resp.Header.Get("Content-Encoding") == "gzip" || UrlHasExtension(u, ".gz")

	switch {

	case gz:
		r0, err := gzip.NewReader(r)
		if err != nil {
			return retry()
		}
		return r0, nil

	case bz2:
		return &readCloser{bzip2.NewReader(r)}, nil
	}

	return r, nil
}

type readCloser struct {
	io.Reader
}

func (r *readCloser) Close() error {
	return nil
}

func (m *MapTool) emrMarker() {
}

type ReduceTool struct {
	reducer     Reducer
	name        string
	description string
	hidden      bool
}

func (m *ReduceTool) emrMarker() {
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

const TICKER = 60 * time.Second

func (m *ReduceTool) Run(args []string) {
	go runTicker("reduce", TICKER)
	runStreamingReducer(m.reducer)
}

func marshal(i fmt.Stringer) ([]byte, error) {
	return []byte(fmt.Sprintf(`{"type":"%T"}`, i)), nil
}

type IdentityMapperTool struct {
	Id      string
	Taglist []string
}

func (m *IdentityMapperTool) emrMarker() {
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
	io.Copy(os.Stdout, os.Stdin)
}

func IdentityMap(ctx MapContext) {
	for kv := range ctx.Input {
		ctx.Collector <- kv
	}
}

func IdentityReduce(ctx ReduceContext) {
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

func (m *IdentityReducerTool) emrMarker() {
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
	io.Copy(os.Stdout, os.Stdin)
}

func IntegerSumReduce(ctx ReduceContext) {
	for j := range ctx.Input {
		var count int64
		for v := range j.Values {
			i, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				count += i
			}
		}
		ctx.Collector <- KeyValue{Key: j.Key, Value: strconv.FormatInt(count, 10)}
	}
}

type FloatSumReducer struct {
	Format string
}

func (f FloatSumReducer) Reduce(ctx ReduceContext) {
	for j := range ctx.Input {
		var total float64
		for v := range j.Values {
			i, err := strconv.ParseFloat(v, 64)
			if err == nil {
				total += i
			}
		}
		ctx.Collector <- KeyValue{Key: j.Key, Value: fmt.Sprintf(f.Format, total)}
	}
}

type Slurper struct {
}

func (*Slurper) Name() string {
	return "slurp,play with slurper"
}
func (t *Slurper) Run(args []string) {
	fmt.Println("slurping...")
	var lines int
	SlurpLines(os.Stdin, func(line string) {
		fmt.Printf("%q\n", line)
		lines++
	})
	fmt.Printf("%d lines\n", lines)
}

// reads all lines, returns error, or nil on EOF
func SlurpLines(r io.Reader, f func(string)) error {
	b := bufio.NewReader(r)
	for {
		line, err := b.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		line = trimEOLs(line)
		if len(line) > 0 {
			f(line)
		}
		if err == io.EOF {
			return nil
		}
	}
}

// possibly a candidate for optimization?
func trimEOLs(line string) string {
	done := false
	for !done {
		switch {
		case strings.HasSuffix(line, "\r") || strings.HasSuffix(line, "\n"):
			line = line[:len(line)-1]
		case strings.HasPrefix(line, "\r") || strings.HasPrefix(line, "\n"):
			line = line[1:]
		default:
			done = true
		}
	}
	return line
}

func (kv KeyValue) String() string {
	if len(kv.Value) == 0 {
		return fmt.Sprintf("%s", kv.Key)
	} else {
		return fmt.Sprintf("%s\t%s", kv.Key, kv.Value)
	}
}

func ReassembleLine(kv KeyValue) string {
	return kv.String()
}

func ParseLine(line string) KeyValue {
	return ParseLineSep(line, "\t")
}

func ParseLineSep(line, sep string) KeyValue {
	i := strings.Index(line, sep)
	if i >= 0 {
		key := line[:i]
		value := line[i+1:]
		return KeyValue{Key: key, Value: value}
	} else {
		return KeyValue{Key: line}
	}
}
