package tool

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

func MyTestCommand(parser FlagParser) {
	var arg string
	parser(func(f *flag.FlagSet) {
		f.StringVar(&arg, "", "", "")
	})
}

type Command interface {
	Name() string
	Description() string
	Children() Commands
	Add(Command)
	Run(FlagParser)
}

type CommandFunc func(p FlagParser)

type FlagParser func(func(f *flag.FlagSet))

func Wrapper(name, desc string, f CommandFunc) Command {
	return &command{
		name:    name,
		desc:    desc,
		command: f,
	}
}

type command struct {
	name     string
	desc     string
	children Commands
	command  CommandFunc
}

func (c command) Name() string {
	return c.name
}

func (c command) Description() string {
	return c.desc
}

func (c *command) Add(x Command) {
	if strings.HasPrefix(x.Name(), "-") {
		panic("illegal name: " + x.Name())
	}
	c.children = append(c.children, x)
	sort.Sort(c.children)
}

func (c command) Children() Commands {
	out := make(Commands, len(c.children))
	if n := copy(out, c.children); n != len(c.children) {
		panic("couldn't copy")
	}
	return out
}

func (c *command) Run(p FlagParser) {
	c.command(p)
}

func (c *command) runTool(f FlagParser) {

	commands := all(c)

	exe := filepath.Base(os.Args[0])
	nargs := len(os.Args)
	switch {

	case nargs == 1 || (nargs == 2 && (os.Args[1] == c.name || os.Args[1] == "-help")):
		fmt.Printf("%s: %s\n", c.Name(), c.Description())
		for _, c := range c.children {
			var desc string
			if n := len(c.Children()); n == 0 {
				desc = c.Description()
			} else {
				desc = fmt.Sprintf("%s (%d subtools)", c.Description(), n)
			}
			fmt.Printf("  %s %s — %s\n", exe, c.Name(), desc)
		}

	case nargs >= 2:
		name := os.Args[1]
		sub := commands.Find(name)
		if sub == nil {
			log.Fatalf("no such command: %q", name)
		}
		fp := func(f func(f *flag.FlagSet)) {
			fs := flag.NewFlagSet(fmt.Sprintf("%s %s", exe, name), flag.ExitOnError)
			f(fs)
			fs.Parse(os.Args[2:])
		}
		sub.Run(fp)

	default:
		panic("illegal")
	}
}

func all(cmd Command) (out Commands) {
	m := make(map[string]Command)
	_all(cmd, m)
	for _, v := range m {
		out = append(out, v)
	}
	sort.Sort(out)
	return
}

func _all(cmd Command, m map[string]Command) {
	if _, ok := m[cmd.Name()]; ok {
		panic(fmt.Errorf("duplicate command: %q", cmd.Name()))
	}
	m[cmd.Name()] = cmd
	for _, c := range cmd.Children() {
		_all(c, m)
	}
}

func NewSetup(name, desc string) Command {
	var c *command
	c = &command{
		name: name,
		desc: desc,
	}
	c.command = c.runTool
	return c
}

type Commands []Command

func (c Commands) Find(name string) Command {
	for _, x := range c {
		if x.Name() == name {
			return x
		}
	}
	return nil
}

func (c Commands) Len() int {
	return len(c)
}
func (c Commands) Less(i, j int) bool {
	return c[i].Name() < c[j].Name()
}
func (c Commands) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}
