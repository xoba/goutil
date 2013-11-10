package emr

import (
	"bufio"
	"code.google.com/p/go-uuid/uuid"
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/xoba/goutil/aws"
	"github.com/xoba/goutil/aws/s3"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

type MonFlow struct {
	Auth map[string]aws.Auth
}

func NewMonFlow(a aws.Auth) *MonFlow {
	m := make(map[string]aws.Auth)
	m["default"] = a
	return &MonFlow{m}
}

func (m *MonFlow) Name() string {
	return "monflow,monitor an emr job flow"
}

func (m *MonFlow) Run(args []string) {
	flow, a := getFlowAndAuth(args, m.Auth)
	for {
		r := FetchFlow(a, flow)
		switch r.State {
		case "STARTING":
			fmt.Println("starting")
		case "RUNNING":
			fmt.Println("running")
		case "SHUTTING_DOWN":
			fmt.Println("shutting down")
		case "COMPLETED":
			fmt.Println("completed")
		default:
			fmt.Printf("default: %s\n", r.State)
		}
		time.Sleep(10 * time.Second)
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
	r := FetchFlow(a, flow)

	if buf, err := json.MarshalIndent(r, "", "  "); err == nil {
		fmt.Println(string(buf))

		for i, s := range r.Steps {
			fmt.Printf("step %d: %v\n", i, s.Output())
		}
	}

	if len(r.MasterDNS) > 0 {

		dir := fmt.Sprintf("/run/shm/chrome/chrome_%s", uuid.New())

		os.MkdirAll(dir, os.ModePerm)

		f, err := os.Create(dir + "/First Run")
		check(err)
		f.Close()

		cmd := exec.Command("google-chrome",
			fmt.Sprintf("--user-data-dir=%s", dir),
			"--window-size=1280,1024", fmt.Sprintf("http://%s:9100", r.MasterDNS))

		cmd.Dir = "/tmp"

		cmd.Start()

	}

}

func FetchFlow(a aws.Auth, flow string) *FlowsResponse {
	v := make(url.Values)

	v.Set("Action", "DescribeJobFlows")
	v.Set("JobFlowIds.member.1", flow)

	u := createSignedURL(a, v)

	res := runReq(u)

	var r FlowsResponse

	xml.Unmarshal(res, &r)

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

func LoadLines(ss3 s3.Interface, output *StepLocation, f func(string, *KeyValue)) {
	decider := func(string) bool {
		return true
	}
	LoadLines2(ss3, output, runtime.NumCPU(), decider, f)
}
func LoadLines2(ss3 s3.Interface, output *StepLocation, threads int, fileDecider func(string) bool, f func(string, *KeyValue)) {
	var wg, wg2 sync.WaitGroup
	ch2 := make(chan *FileKeyValue)
	ch := make(chan s3.Object)
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
				fn := o.Bucket + "/" + o.Key
				if fileDecider(fn) {
					r, err := ss3.Get(s3.GetRequest{o})
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

type FileKeyValue struct {
	Filename string
	Item     *KeyValue
}

// randomize order of each listing batch
func List(ss3 s3.Interface, output *StepLocation, ch chan s3.Object) {
	var marker string
	for {
		r, err := ss3.List(s3.ListRequest{MaxKeys: 1000, Bucket: output.Bucket, Prefix: output.Prefix, Marker: marker})
		if err != nil {
			panic(err)
		}

		p := rand.Perm(len(r.Contents))

		for i := 0; i < len(r.Contents); i++ {
			v := r.Contents[p[i]]
			ch <- s3.Object{Bucket: output.Bucket, Key: v.Key}
		}

		if r.IsTruncated {
			marker = r.Contents[len(r.Contents)-1].Key
		} else {
			break
		}
	}
	close(ch)
}
