// manages main executable as agglomeration of tools.
package tool

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
)

type Interface interface{}

type CanRun interface {
	Run(args []string)
}

type RunFunc func(args []string)

func (r RunFunc) Run(args []string) {
	r(args)
}

type HasDescription interface {
	Description() string
}

type HasName interface {
	Name() string // no dots before optional comma, description after
}

func Name(i Interface) string {
	name, _ := splitOnFirstComma(fullname(i))
	if len(name) > 0 {
		return name
	} else {
		return fullname(i)
	}
}

func fullname(i Interface) string {
	if n, ok := i.(HasName); ok {
		return strings.TrimSpace(n.Name())
	} else {
		return strings.TrimSpace(fmt.Sprintf("%v", i))
	}
}

func splitOnFirstComma(line string) (string, string) {
	if parts := strings.SplitN(line, ",", 2); len(parts) == 1 {
		return strings.TrimSpace(line), ""
	} else {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
}

func Description(i Interface) string {
	if e, ok := i.(HasDescription); ok {
		return e.Description()
	} else if _, d := splitOnFirstComma(fullname(i)); len(d) > 1 {
		return d
	}
	return ""
}

func ConditionalRun(msg string, def bool, runTrue, runFalse func()) {
	if r, err := ConfirmYorN(msg, def); r && err == nil {
		runTrue()
	} else {
		runFalse()
	}
}

func ConfirmYorN(msg string, def bool) (bool, error) {
	d := func() string {
		if def {
			return "Y"
		} else {
			return "N"
		}
	}()

	fmt.Printf("%s [%s] ", msg, d)
	var resp string
	_, err := fmt.Scanf("%s", &resp)
	if err != nil {
		return false, err
	}
	if len(resp) == 0 {
		resp = d
	}
	resp = strings.ToLower(resp)
	return !strings.Contains(resp, "n") && strings.Contains(resp, "y"), nil
}

func Run() {

	if len(os.Args) < 2 {
		ListTools(roots)
		return
	}

	name := os.Args[1]

	if children, ok := tree[name]; ok {
		ListTools(children)
		return
	} else {
		t, ok := tools[name]
		if !ok {
			fmt.Fprintf(os.Stderr, "no such tool: %q\n", name)
			os.Exit(1)
		}
		RunInterface(t, os.Args[2:])
	}
}

func ToolsForNames(names []string) []Interface {
	var list []Interface
	for _, r := range roots {
		list = append(list, tools[r])
	}
	return list
}

func ListTools(names []string) {
	var max int
	for _, name := range names {
		if len(name) > max {
			max = len(name)
		}
	}
	sort.Strings(names)
	spaces := func(n int) string {
		buf := new(bytes.Buffer)
		for i := 0; i < n; i++ {
			buf.WriteRune(' ')
		}
		return buf.String()
	}
	for _, n := range names {
		var dir string
		if c, ok := tree[n]; ok {
			if len(c) > 1 {
				dir = fmt.Sprintf("%d subtools", len(c))
			} else {
				dir = "one subtool"
			}
		}
		fmt.Printf("%s ", path.Base(os.Args[0]))
		fmt.Printf("%s %s # ", n, spaces(max-len(n)))
		if d := Description(tools[n]); len(d) > 0 {
			fmt.Printf("%s ", d)
		}
		if len(dir) > 0 {
			fmt.Printf("(%s)", dir)
		}
		fmt.Println()
	}
}

func RunInterface(i Interface, args []string) {
	if r, ok := i.(CanRun); ok {
		r.Run(args)
	} else {
		fmt.Fprintf(os.Stderr, fmt.Sprintf("oops, can't run %q\n", Name(i)))
		os.Exit(1)
	}
}

var (
	roots []string
	tools map[string]Interface = make(map[string]Interface)
	tree  map[string][]string  = make(map[string][]string)
)

func Register(r Interface) {
	register(r)
	roots = append(roots, Name(r))
}

func RegisterChild(parent string, child Interface) {
	register(child)
	tree[parent] = append(tree[parent], Name(child))
}

func register(r Interface) {
	name := Name(r)
	_, ok := tools[name]
	if ok {
		panic("tools with duplicate names: " + name)
	}
	tools[name] = r
}

// gets all tool names
func Names() (out []string) {
	for k := range tools {
		out = append(out, k)
	}
	return
}

// gets the tool with given name
func ForName(name string) Interface {
	return tools[name]
}
