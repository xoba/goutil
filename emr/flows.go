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
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"sync"
)

type ShowFlow struct {
	Auth aws.Auth
}

func (t *ShowFlow) Tags() []string {
	return []string{}
}
func (m *ShowFlow) Name() string {
	return "flow"
}
func (m *ShowFlow) Description() string {
	return "debug an emr job flow"
}

func (m *ShowFlow) Run(args []string) {

	var flow string
	flags := flag.NewFlagSet(m.Name(), flag.ExitOnError)
	flags.StringVar(&flow, "id", "", "the job flow to debug")
	flags.Parse(args)

	r := FetchFlow(m.Auth, flow)

	if buf, err := json.MarshalIndent(r, "", "  "); err == nil {
		fmt.Println(string(buf))

		for i, s := range r.Steps {
			fmt.Printf("step %d: %v\n", i, s.Output())
		}
	}

	if len(r.MasterDNS) > 0 {

		cmd := exec.Command("google-chrome",
			fmt.Sprintf("--user-data-dir=/run/shm/chrome/chrome_%s",
				uuid.New()),
			"--app-window-size=1280,1024", fmt.Sprintf("--app=http://%s:9100", r.MasterDNS))

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

type StepOutput struct {
	Bucket string
	Prefix string
}

func (s *StepMember) Output() *StepOutput {
	var next bool
	for _, x := range s.Args {
		if next {
			u, err := url.Parse(x)
			if err != nil {
				return nil
			}
			return &StepOutput{Bucket: u.Host, Prefix: u.Path[1:]}
		}
		if x == "-output" {
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

func LoadLines(ss3 s3.Interface, output *StepOutput, f func(string, *KeyValue)) {
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
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for o := range ch {
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
						Filename: o.Bucket + "/" + o.Key,
						Item:     &kv,
					}
				}
				if err := scanner.Err(); err != nil {
					panic(err)
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

func List(ss3 s3.Interface, output *StepOutput, ch chan s3.Object) {
	var marker string
	for {
		r, err := ss3.List(s3.ListRequest{MaxKeys: 1000, Bucket: output.Bucket, Prefix: output.Prefix, Marker: marker})
		if err != nil {
			panic(err)
		}
		for _, v := range r.Contents {
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