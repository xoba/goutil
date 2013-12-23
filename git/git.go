// work with git
package git

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"
)

type TagTool struct {
}

func (TagTool) Name() string {
	return "tags,dump git tags"
}
func (TagTool) Run(args []string) {
	tags, err := Tags()
	check(err)
	for _, x := range tags {
		fmt.Println(marshal(x))
	}
}

func marshal(i interface{}) string {
	buf, err := json.MarshalIndent(i, "", "  ")
	check(err)
	return string(buf)
}

type Tag struct {
	Name        string
	Errors      []error `json:",omitempty"`
	Tagger      string  `json:",omitempty"`
	Author      string  `json:",omitempty"`
	TagDate     time.Time
	Commit      string
	CommitTime  time.Time
	IsAnnotated bool
}

type TagList []Tag

func (t TagList) Len() int {
	return len(t)
}
func (t TagList) Less(i, j int) bool {
	return t[i].CommitTime.Before(t[j].CommitTime)
}
func (t TagList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func Tags() (TagList, error) {
	var list TagList
	err := lineRunner("git", []string{"tag"}, func(line string) {
		list = append(list, GetTag(line))
	})
	sort.Sort(sort.Reverse(list))
	return list, err
}

func GetTag(name string) Tag {

	var errs []error

	add := func(e error) {
		if e != nil {
			errs = append(errs, e)
		}
	}

	tag := Tag{
		Name: name,
	}

	lineCount := 0
	dates := 0
	add(lineRunner("git", []string{"show", "--format=medium", name}, func(line string) {

		lineCount++

		starts := func(prefix string) bool {
			return strings.HasPrefix(line, prefix)
		}

		value := func() string {
			return strings.TrimSpace(line[strings.Index(line, " "):])
		}

		dt := func() (out time.Time) {
			out, _ = time.Parse("Mon Jan 02 15:04:05 2006 -0700", value())
			out = out.UTC()
			return
		}

		if lineCount == 1 {
			switch {
			case starts("tag"):
				tag.IsAnnotated = true
			case starts("commit"):
				tag.IsAnnotated = false
				tag.Commit = value()
			default:
				add(fmt.Errorf("illegal first line: %s", line))
			}
			return
		}

		switch {
		case starts("commit"):
			tag.Commit = value()
		case dates == 0 && starts("Date"):
			tag.TagDate = dt()
			dates++
		case dates == 1 && starts("Date"):
			tag.CommitTime = dt()
			dates++
		case starts("Author"):
			tag.Author = value()
		case starts("Tagger"):
			tag.Tagger = value()
		}
	}))

	if tag.CommitTime.IsZero() {
		tag.CommitTime = tag.TagDate
	}

	tag.Errors = errs

	return tag
}

func lineRunner(command string, args []string, f func(line string)) error {
	cmd := exec.Command(command, args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Start()
	s := bufio.NewScanner(out)
	for s.Scan() {
		f(s.Text())
	}
	if s.Err() != nil {
		return err
	}
	return cmd.Wait()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
