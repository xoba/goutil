package tool

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
)

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

func Run() {

	if len(os.Args) < 2 {
		fmt.Println("nothing to run, see options; -help shows more:\n")

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

			rows = append(rows, row)
		}

		cols := strings.Split("command,description,code", ",")

		if hasTags {
			cols = append(cols, "tags")
		}

		fmt.Println(FormatTextTable(false, " ", cols, rows))

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
