package emr

import (
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/xoba/goutil/aws"
	"net/url"
	"os/exec"
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
