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

// args are the name of tool to run forever, followed by its args
func Run(args []string, delay time.Duration, f func(error)) {
	path, err := os.Readlink("/proc/self/exe")
	if err != nil {
		panic(err)
	}
	log.Printf("\"%s %s\"\n", path, strings.Join(args, " "))
	for {
		start := time.Now()
		if err := try(path, args); err != nil {
			f(err)
			log.Printf("got error: %v\n", err)
		}
		end := time.Now()
		if end.Sub(start) < delay {
			time.Sleep(delay)
		}
	}
}

func try(path string, args []string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("oops, recovered running %s: %v", path, r)
		}
	}()
	cmd := exec.Command(path, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	return err
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
