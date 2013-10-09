package tool

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

func FormatTextTable(borders bool, prefix string, cols []string, rows []map[string]string) string {

	if cols == nil {
		m := make(map[string]bool)
		for _, x := range rows {
			for k := range x {
				m[k] = true
			}
		}
		for k := range m {
			cols = append(cols, k)
		}
		sort.Strings(cols)
	}

	{
		header := make(map[string]string)
		for _, x := range cols {
			header[x] = strings.ToUpper(x) + ":"
		}

		var rows2 []map[string]string
		rows2 = append(rows2, header)
		for _, x := range rows {
			rows2 = append(rows2, x)
		}

		rows = rows2
	}

	var buf bytes.Buffer

	print := func(x string) {
		buf.Write([]byte(fmt.Sprint(x)))
	}
	printw := func(width int, x string) {
		for {
			if len(x) == width {
				break
			} else {
				x = x + " "
			}
		}
		buf.Write([]byte(fmt.Sprint(x)))
	}

	widths := make(map[string]int)

	for _, row := range rows {
		for k, v := range row {
			if len(v) > widths[k] {
				widths[k] = len(v)
			}
		}
	}

	if borders {

		writeLine := func() {
			print(prefix)
			print("+")
			for _, c := range cols {
				w := widths[c]
				for i := 0; i < w; i++ {
					print("-")
				}
				print("+")
			}
			print("\n")
		}

		writeRow := func(row map[string]string) {
			print(prefix)
			print("|")
			for _, c := range cols {
				v := row[c]
				printw(widths[c], v)
				print("|")
			}
			print("\n")
			writeLine()
		}

		writeLine()
		for _, r := range rows {
			writeRow(r)
		}

	} else {

		writeLine := func() {
			print(prefix)
			for i, c := range cols {
				w := widths[c]
				if i > 0 {
					print("+")
				}
				for i := 0; i < w+1; i++ {
					print("-")
				}
				if i > 0 {
					print("-")
				}
			}
			print("\n")
		}

		writeRow := func(row map[string]string) {
			print(prefix)
			for i, c := range cols {
				if i > 0 {
					print(" | ")
				}
				v := row[c]
				printw(widths[c], v)
			}
			print("\n")
		}

		for i, r := range rows {
			if i == 1 {
				writeLine()
			}
			writeRow(r)
		}
	}

	return string(buf.Bytes())
}
