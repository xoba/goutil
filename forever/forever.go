// runs a tool forever
package forever

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

const SELF_LINK = "/proc/self/exe"
const MIN = time.Second

// args are the name of tool to run forever, followed by its args
func Run(args []string, f func(error)) {
	path, err := os.Readlink(SELF_LINK)
	if err != nil {
		panic(err)
	}
	log.Printf("%s %v # running forever...\n", path, quote(args))
	for {
		start := time.Now()
		if err := try(path, args); err != nil {
			f(err)
			log.Printf("got error: %v\n", err)
		}
		end := time.Now()
		if end.Sub(start) < MIN {
			time.Sleep(MIN)
		}
	}
}

func try(path string, args []string) error {
	cmd := exec.Command(path, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
