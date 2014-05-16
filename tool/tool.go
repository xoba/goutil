// manages main executable as agglomeration of tools.
package tool

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

type Interface interface {
	Name() string
	Run(args []string)
}

type HasDescription interface {
	Description() string
}

func Name(i Interface) string {
	parts := strings.Split(i.Name(), ",")
	if len(parts) > 1 {
		return parts[0]
	} else {
		return i.Name()
	}
}

func Description(i Interface) string {
	parts := strings.Split(i.Name(), ",")
	if len(parts) > 1 {
		return strings.TrimSpace(parts[1])
	} else {
		return rawDesc(i)
	}
}

func rawDesc(i Interface) string {
	e, ok := i.(HasDescription)
	if ok {
		return e.Description()
	} else {
		return ""
	}
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

func Query(message, def string) (string, error) {
	fmt.Printf("%s [%s] ", message, def)
	var resp string
	_, err := fmt.Scanf("%s", &resp)
	if err != nil {
		return "", err
	}
	return resp, nil
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
		var names []string
		for k := range tools {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Printf("%s %s\t\t# %s\n", os.Args[0], n, Description(tools[n]))
		}
		return
	}
	name := os.Args[1]
	t, ok := tools[name]
	if !ok {
		fmt.Fprintf(os.Stderr, "no such tool: %q\n", name)
		os.Exit(1)
	}
	t.Run(os.Args[2:])
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
