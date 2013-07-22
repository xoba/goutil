package tool

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
)

type Build struct {
	Version string
	Commit  string // the commit id
	Url     string // identifies the commit (like a github url)
	BuildId string
	Status  string
	Built   time.Time
}

const (
	BUILT_FORMAT = "2006-01-02T15:04:05Z"
)

func (b Build) FormatBuilt() string {
	return b.Built.Format(BUILT_FORMAT)
}

type Interface interface {
	Name() string
	Description() string
	Tags() []string
	Run(args []string)
}

func ConfirmYorN(msg string) bool {
	fmt.Printf("%s [N] ", msg)
	var resp string
	_, err := fmt.Scanf("%s", &resp)
	if err != nil {
		return false
	}
	if len(resp) == 0 {
		resp = "n"
	}
	resp = strings.ToLower(resp)
	return !strings.Contains(resp, "n") && strings.Contains(resp, "y")
}

func SummarizeFlags(fs *flag.FlagSet) {
	fmt.Println("running with:")
	fs.VisitAll(func(f *flag.Flag) {
		fmt.Printf("\t-%s=\"%s\" (%s", f.Name, f.Value.String(), f.Usage)
		if f.Value.String() != f.DefValue {
			fmt.Printf(", different than default of \"%s\"", f.DefValue)
		}
		fmt.Println(")")
	})
}

func Run(b Build) {

	var hidden, version bool
	var pathUrl string

	flag.BoolVar(&version, "version", false, "detailed version information")
	flag.BoolVar(&hidden, "hidden", false, "show hidden tools")
	flag.StringVar(&pathUrl, "pathurl", "", "adds path onto build url")
	flag.Parse()

	switch {

	case len(pathUrl) > 0:
		p := path.Clean("/" + pathUrl)
		fmt.Printf("%s%s\n", b.Url, p)

	case len(os.Args) < 2 || hidden:

		fmt.Printf("v.%s: nothing to run, see options; -help shows more:\n\n", b.Version)

		var names []string
		for k, _ := range tools {
			names = append(names, k)
		}

		sort.Strings(names)

		var hasTags bool
		var rows []map[string]string

		for _, k := range names {

			v := tools[k]

			row := make(map[string]string)

			row["command"] = os.Args[0] + " " + k
			row["description"] = v.Description()

			row["code"] = fmt.Sprintf("%v", reflect.TypeOf(v))

			tags := v.Tags()
			if len(tags) > 0 {
				sort.Strings(tags)
				row["tags"] = strings.Join(tags, ", ")
				hasTags = true
			}

			if hidden || !strings.Contains(row["tags"], "hidden") {
				rows = append(rows, row)
			}
		}

		cols := strings.Split("command,description,code", ",")

		if hasTags {
			cols = append(cols, "tags")
		}

		fmt.Println(FormatTextTable(false, " ", cols, rows))

	default:

		if version {

			p := KeyValuePrinter{values: make(map[string]string)}

			p.Line("version", fmt.Sprintf("%s / go %s", b.Version, runtime.Version()))
			p.Line("commit", b.Commit)
			p.Line("url", b.Url)
			p.Line("build id", b.BuildId)
			p.Line("built", fmt.Sprintf("%s (%v ago)", b.Built.Format("2006-01-02T15:04:05Z"), time.Now().Sub(b.Built)))
			p.Line("status", b.Status)

			p.Print()

		} else {
			name := os.Args[1]
			t, ok := tools[name]
			if !ok {
				fmt.Printf("no such tool: %s\n", name)
				os.Exit(1)
			}

			t.Run(os.Args[2:])
		}
	}
}

type KeyValuePrinter struct {
	keys   []string
	values map[string]string
}

func (p *KeyValuePrinter) Line(k, v string) {
	p.keys = append(p.keys, k)
	p.values[k] = v
}
func (p *KeyValuePrinter) Print() {
	var max int
	for _, k := range p.keys {
		if len(k) > max {
			max = len(k)
		}

	}
	for _, k := range p.keys {
		v := p.values[k]
		fmt.Printf("%-"+fmt.Sprintf("%d", max+1)+"s ", k+":")

		for i, x := range strings.Split(v, "\n") {
			if i > 0 {
				for j := 0; j < max+2; j++ {
					fmt.Print(" ")
				}
			}
			fmt.Printf("%s\n", x)
		}

	}

}

var tools map[string]Interface = make(map[string]Interface)

func Register(r Interface) {
	name := r.Name()
	_, ok := tools[name]
	if ok {
		panic("tools with duplicate names: " + name)
	}
	tools[name] = r
}

func NewSequence(name, desc string, tools ...Interface) *Sequence {
	return &Sequence{name: name, desc: desc, tools: tools}
}

type Sequence struct {
	name, desc string
	tools      []Interface
}

func (st *Sequence) Tags() (out []string) {
	m := make(map[string]bool)
	for _, x := range st.tools {
		for _, y := range x.Tags() {
			m[y] = true
		}
	}
	for k, _ := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return []string{}
}
func (st *Sequence) Name() string {
	return st.name
}
func (st *Sequence) Description() string {
	var buf bytes.Buffer
	b := bufio.NewWriter(&buf)
	b.WriteString(st.desc)

	b.WriteString(" (seq: ")
	var out []string
	for _, x := range st.tools {
		out = append(out, x.Name())
	}
	b.WriteString(strings.Join(out, ", "))
	b.WriteString(")")

	b.Flush()
	return string(buf.Bytes())
}
func (st *Sequence) Run(args []string) {
	for _, x := range st.tools {
		fmt.Printf("running %s...\n", x.Name())
		x.Run(args)
	}
}
