package emr

import (
	"code.google.com/p/go-uuid/uuid"
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

	v := make(url.Values)

	v.Set("Action", "DescribeJobFlows")
	v.Set("JobFlowIds.member.1", flow)

	u := createSignedURL(m.Auth, v)

	res := debugReq(u)

	var r Response
	xml.Unmarshal(res, &r)

	if len(r.MasterDNS) > 0 {

		cmd := exec.Command("google-chrome",
			fmt.Sprintf("--user-data-dir=/run/shm/chrome/chrome_%s",
				uuid.New()),
			"--app-window-size=1280,1024", fmt.Sprintf("--app=http://%s:9100", r.MasterDNS))

		cmd.Dir = "/tmp"

		fmt.Println(cmd.Path)
		fmt.Printf("%#v\n", cmd.Args)

		cmd.Start()
	} else {
		fmt.Println("# master dns not available")
	}

}

type Response struct {
	MasterDNS string `xml:"DescribeJobFlowsResult>JobFlows>member>Instances>MasterPublicDnsName"`
}
