package emr

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/xoba/goutil/aws"
	"github.com/xoba/goutil/aws/s3"
)

type FlowListener func(id, state string)

type MonFlow struct {
	Auth map[string]aws.Auth
	FlowListener
}

func NewMonFlow(a map[string]aws.Auth, l FlowListener) *MonFlow {
	return &MonFlow{a, l}
}

func (m *MonFlow) Name() string {
	return "monflow,monitor an emr job flow"
}

func (m *MonFlow) Run(args []string) {
	flow, a := getFlowAndAuth(args, m.Auth)
	for {
		r := FetchFlow(a, flow)
		log.Printf("state of %s = %q\n", flow, r.State)
		m.FlowListener(flow, r.State)
		if r.State == "COMPLETED" {
			return
		}
		time.Sleep(30 * time.Second)
	}
}

// should have a "default" key
type ShowFlow struct {
	Auth map[string]aws.Auth
}

func NewShowFlow(a aws.Auth) *ShowFlow {
	m := make(map[string]aws.Auth)
	m["default"] = a
	return &ShowFlow{m}
}

func (m *ShowFlow) Name() string {
	return "flow"
}
func (m *ShowFlow) Description() string {
	return "debug an emr job flow"
}

func getFlowAndAuth(args []string, m map[string]aws.Auth) (string, aws.Auth) {
	var flow, auth string
	flags := flag.NewFlagSet("flow", flag.ExitOnError)
	flags.StringVar(&flow, "id", "", "the job flow to debug")
	flags.StringVar(&auth, "auth", "default", "the authorization to choose")
	flags.Parse(args)
	a := func() aws.Auth {
		switch {
		case len(m) == 0:
			panic("no authorizations")
		case len(m) == 1:
			for _, v := range m {
				return v
			}
		}
		return m[auth]
	}()
	return flow, a
}

func (m *ShowFlow) Run(args []string) {
	flow, a := getFlowAndAuth(args, m.Auth)
	if len(flow) == 0 {
		log.Fatal("needs flow id!")
	}
	r := FetchFlow(a, flow)
	if buf, err := json.MarshalIndent(r, "", "  "); err == nil {
		fmt.Println(string(buf))

		for i, s := range r.Steps {
			fmt.Printf("step %d: %v\n", i, s.Output())
		}
	}
	if len(r.MasterDNS) > 0 {
		cmd := exec.Command("xdg-open", fmt.Sprintf("http://%s:9100", r.MasterDNS))
		cmd.Dir = "/tmp"
		cmd.Start()
	}
}

type RunFlowResponse struct {
	FlowId string `xml:"RunJobFlowResult>JobFlowId"`
}

func ParseEmrResponse(r io.Reader) (*RunFlowResponse, error) {
	var x RunFlowResponse
	d := xml.NewDecoder(r)
	err := d.Decode(&x)
	if err != nil {
		return nil, err
	}
	return &x, nil
}

func FetchFlow(a aws.Auth, flow string) *FlowsResponse {
	v := make(url.Values)
	v.Set("Action", "DescribeJobFlows")
	v.Set("JobFlowIds.member.1", flow)
	u := createSignedURL(a, v)
	resp, err := runReq(u)
	check(err)
	defer resp.Close()
	d := xml.NewDecoder(resp)
	var r FlowsResponse
	check(d.Decode(&r))
	return &r
}

type StepMember struct {
	Name string   `xml:"StepConfig>Name"`
	Args []string `xml:"StepConfig>HadoopJarStep>Args>member"`
}

type StepLocation struct {
	Bucket string
	Prefix string
}

func (s *StepMember) Input() *StepLocation {
	return s.location("-input")
}

func (s *StepMember) Output() *StepLocation {
	return s.location("-output")
}

func (s *StepMember) location(name string) *StepLocation {
	var next bool
	for _, x := range s.Args {
		if next {
			u, err := url.Parse(x)
			if err != nil {
				return nil
			}
			return &StepLocation{Bucket: u.Host, Prefix: u.Path[1:]}
		}
		if x == name {
			next = true
		}
	}
	return nil
}

type FlowsResponse struct {
	State     string       `xml:"DescribeJobFlowsResult>JobFlows>member>ExecutionStatusDetail>State"`
	MasterDNS string       `xml:"DescribeJobFlowsResult>JobFlows>member>Instances>MasterPublicDnsName"`
	Steps     []StepMember `xml:"DescribeJobFlowsResult>JobFlows>member>Steps>member"`
}

func (f *FlowsResponse) GetStep(name string) *StepMember {
	for _, x := range f.Steps {
		if x.Name == name {
			return &x
		}
	}
	return nil
}

func (f *StepMember) ExtractVars() map[string]string {
	out := make(map[string]string)
	for i, x := range f.Args {
		if x == "-cmdenv" {
			parts := strings.Split(f.Args[i+1], "=")
			if strings.HasPrefix(parts[0], VARS_PREFIX) {
				out[parts[0][len(VARS_PREFIX):]] = parts[1]
			}
		}
	}
	return out
}

func LoadLines(ss3 s3.Interface, output *StepLocation, f func(string, *KeyValue)) {
	decider := func(string) bool {
		return true
	}
	LoadLines2(ss3, output, runtime.NumCPU(), decider, f)
}

type UrlDeciderFunc func(url string) bool

func LoadLines2(ss3 s3.Interface, output *StepLocation, threads int, decider UrlDeciderFunc, f func(string, *KeyValue)) {
	var wg, wg2 sync.WaitGroup
	ch2 := make(chan *FileKeyValue)
	ch := make(chan s3.ListedObject)
	wg2.Add(1)
	go func() {
		for fkv := range ch2 {
			f(fkv.Filename, fkv.Item)
		}
		wg2.Done()
	}()
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			for o := range ch {
				fn := o.Object().Url()
				if decider(fn) {
					r, err := ss3.Get(s3.GetRequest{Object: o.Object()})
					check(err)
					defer r.Close()
					if strings.HasSuffix(o.Key, ".gz") {
						r, err = gzip.NewReader(r)
						check(err)
					}

					scanner := bufio.NewScanner(r)
					for scanner.Scan() {
						kv := ParseLine(scanner.Text())
						ch2 <- &FileKeyValue{
							Filename: fn,
							Item:     &kv,
						}
					}
					if err := scanner.Err(); err != nil {
						panic(err)
					}
				}
			}
			wg.Done()
		}()
	}
	List(ss3, output, ch)
	wg.Wait()
	close(ch2)
	wg2.Wait()
}

// enables transactional processing of files
func LoadLines3(ss3 s3.Interface, output *StepLocation, threads int, proc FileProcessor) {
	var wg sync.WaitGroup
	ch := make(chan s3.ListedObject)
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			for o := range ch {
				fn := o.Object().Url()
				p := proc.ForFile(fn, o.Size)
				for p != nil {
					r, err := ss3.Get(s3.GetRequest{Object: o.Object()})
					if err != nil {
						if p = proc.Failure(fn, o.Size, err); p != nil {
							continue
						} else {
							break
						}
					}
					if strings.HasSuffix(o.Key, ".gz") {
						r, err = gzip.NewReader(r)
						check(err)
					}
					scanner := bufio.NewScanner(r)
					for scanner.Scan() {
						kv := ParseLine(scanner.Text())
						p(&kv)
					}
					if err := scanner.Err(); err != nil {
						r.Close()
						if p = proc.Failure(fn, o.Size, err); p != nil {
							continue
						} else {
							break
						}
					} else {
						proc.Success(fn)
						p = nil
					}
					r.Close()
				}
			}
			wg.Done()
		}()
	}
	List(ss3, output, ch)
	wg.Wait()
}

type KeyValueProcessor func(*KeyValue)

// each KeyValueProcessor called from just a single thread
type FileProcessor interface {

	// should return function to process keyvalue's from file, or nil if no processing
	ForFile(url string, size int) KeyValueProcessor

	// indicates the given file was successfully processed
	Success(url string)

	// called upon failure processing a file; should return a processor if we want to retry
	Failure(url string, size int, err error) KeyValueProcessor
}

type FileKeyValue struct {
	Filename string
	Item     *KeyValue
}

// randomize order of each listing batch
func List(ss3 s3.Interface, output *StepLocation, ch chan s3.ListedObject) {
	var marker string
	for {
		r, err := ss3.List(s3.ListRequest{MaxKeys: 1000, Bucket: output.Bucket, Prefix: output.Prefix, Marker: marker})
		if err != nil {
			panic(err)
		}

		p := rand.Perm(len(r.Contents))

		for i := 0; i < len(r.Contents); i++ {
			v := r.Contents[p[i]]
			ch <- s3.ListedObject{
				ListBucketResultContents: v,
				Bucket: output.Bucket,
			}
		}

		if r.IsTruncated {
			marker = r.Contents[len(r.Contents)-1].Key
		} else {
			break
		}
	}
	close(ch)
}
