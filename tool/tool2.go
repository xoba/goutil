package tool

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"time"
)

type CommandFunc func(f Args)

type Args struct {
	Slice []string
	*flag.FlagSet
}

func (a Args) Parse() error {
	return a.FlagSet.Parse(a.Slice)
}

type Setup struct {
	cmds map[string]command
}

func PlatformInit() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func NewSetup() Setup {
	PlatformInit()
	return Setup{
		cmds: make(map[string]command),
	}
}

func (s Setup) Add(name, desc string, f CommandFunc) error {
	if _, ok := s.cmds[name]; ok {
		return fmt.Errorf("duplicate tool with name: %s", name)
	}
	s.cmds[name] = command{
		name:        name,
		description: desc,
		run:         f,
	}
	return nil
}

func (s Setup) Names() (list []string) {
	for k := range s.cmds {
		list = append(list, k)
	}
	sort.Strings(list)
	return
}

func (s Setup) Run() error {
	if len(os.Args) == 1 || os.Args[1] == "-help" {
		fmt.Printf("%d tool(s):\n", len(s.cmds))
		for i, k := range s.Names() {
			fmt.Printf("  %d. %s — %s\n", i+1, k, s.cmds[k].description)
		}
		return nil
	}
	cmd, ok := s.cmds[os.Args[1]]
	if !ok {
		return fmt.Errorf("unknown tool: %s", os.Args[1])
	}
	cmd.run(Args{
		Slice:   os.Args[2:],
		FlagSet: flag.NewFlagSet("xyz", flag.ExitOnError),
	})
	return nil
}

type command struct {
	name        string
	description string
	run         CommandFunc
}
