// idempotently prepends bin directory to current PATH and prints; e.g.,
// export PATH=`go run src/github.com/xoba/goutil/setpath/setpath.go`
package main

import (
	"fmt"
	"os"
	"path"
	"strings"
)

func main() {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "can't run os.Getwd()")
		os.Exit(1)
	}
	bin := path.Clean(wd + "/bin")
	parts := strings.Split(os.Getenv("PATH"), ":")
	var hasBin bool
	for _, p := range parts {
		if p == bin {
			hasBin = true
		}
	}
	if !hasBin {
		var out []string
		out = append(out, bin)
		out = append(out, parts...)
		parts = out
	}
	fmt.Println(strings.Join(parts, ":"))
}
