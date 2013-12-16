// runs a tool forever
package forever

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const SELF_LINK = "/proc/self/exe"
const MIN = time.Second

// args are the name of tool to run forever, followed by its args
func Run(args []string) {
	path, err := os.Readlink(SELF_LINK)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %v # running forever...\n", path, quote(args))
	for {
		start := time.Now()
		reason, err := try(path, args)
		if err != nil {
			fmt.Printf("got error: %q; %v\n", reason, err)
		}
		end := time.Now()
		if end.Sub(start) < MIN {
			time.Sleep(MIN)
		}
	}
}

func try(path string, args []string) (string, error) {

	wg := new(sync.WaitGroup)

	cmd := exec.Command(path, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "stdout", err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "stderr", err
	}

	err = cmd.Start()
	if err != nil {
		return "start", err
	}

	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		defer stderr.Close()
		_, errs[0] = io.Copy(os.Stderr, stderr)
	}()
	go func() {
		defer wg.Done()
		defer stdout.Close()
		_, errs[1] = io.Copy(os.Stdout, stdout)
	}()

	err = cmd.Wait()
	if err != nil {
		return "wait", err
	}

	wg.Wait()

	for i, e := range errs {
		if e != nil {
			return fmt.Sprintf("err%d", i), e
		}
	}

	return "", nil
}

type Test struct {
}

func (Test) Name() string {
	return "f.test,test how well forever works"
}

func (Test) Run(args []string) {

	fmt.Fprintln(os.Stdout, "writing to stdout")
	fmt.Fprintln(os.Stderr, "writing to stderr")

	if true {
		done := make(chan bool)
		go func() {
			time.Sleep(1 * time.Second)
			fmt.Println("panicking!")
			panic(fmt.Sprintf("panic at %v", time.Now()))
		}()
		<-done
	}

}

func quote(a []string) string {
	var out []string
	for _, s := range a {
		out = append(out, fmt.Sprintf("%q", s))
	}
	return strings.Join(out, " ")
}
