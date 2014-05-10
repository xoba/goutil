package goutil

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

type Tester struct {
}

func (Tester) Name() string {
	return "tester,initializes your tests"
}

func (t Tester) Run(args []string) {

	var root string

	flags := flag.NewFlagSet(t.Name(), flag.ExitOnError)
	flags.StringVar(&root, "root", "", "root package")
	flags.Parse(args)

	tests := make(map[string]bool)

	src := path.Clean(os.Getenv("GOPATH") + "/src/" + root)
	f := func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && path != src {
			dir, err := ioutil.ReadDir(path)
			if err != nil {
				return err
			}
			tests[path] = false
			for _, d := range dir {
				if strings.HasSuffix(d.Name(), "_test.go") {
					tests[path] = true
				}
			}
		}
		return nil
	}
	filepath.Walk(src, f)

	for k, v := range tests {
		fmt.Printf("%5v: %s\n", v, k)
		if !v {
			pkg := path.Base(k)
			tf := path.Clean(k + "/" + pkg + "_test.go")
			f, err := os.Create(tf)
			check(err)
			fmt.Fprintf(f, `package %s

import "testing"

func BogusTest(t *testing.T) {
}
`, pkg)
			f.Close()

			cmd := exec.Command("gofmt", "-w", tf)
			check(cmd.Run())
		}
	}
}
