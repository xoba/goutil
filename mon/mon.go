package mon

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type CpuMonitor struct {
	last              time.Time
	total, idle, self uint64
}

func NewCpuMonitor() (*CpuMonitor, error) {
	idle, total, self, err := getStats()
	if err != nil {
		return nil, err
	}
	return &CpuMonitor{
		last:  time.Now(),
		total: total,
		idle:  idle,
		self:  self,
	}, nil
}

func getStats() (idle, total, self uint64, e error) {
	a, b, err := getCPUSample()
	if err != nil {
		return 0, 0, 0, err
	}
	c, err := getSelfSample()
	if err != nil {
		return 0, 0, 0, err
	}
	return a, b, c, nil
}

func getCPUSample() (idle, total uint64, e error) {
	contents, err := ioutil.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, err
	}
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if fields[0] == "cpu" {
			numFields := len(fields)
			for i := 1; i < numFields; i++ {
				val, err := strconv.ParseUint(fields[i], 10, 64)
				if err != nil {
					fmt.Println("Error: ", i, fields[i], err)
				}
				total += val // tally up all the numbers to get total ticks
				if i == 4 {  // idle is the 5th field in the cpu line
					idle = val
				}
			}
			return idle, total, nil
		}
	}
	return 0, 0, fmt.Errorf("couldn't find cpu line")
}

func getSelfSample() (n uint64, e error) {
	contents, err := ioutil.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, err
	}
	parts := strings.Fields(string(contents))
	f := func(i int) (uint64, error) {
		u, err := strconv.ParseUint(parts[i], 10, 64)
		if err != nil {
			return 0, err
		}
		return u, nil
	}
	utime, err := f(13)
	if err != nil {
		return 0, err
	}
	stime, err := f(14)
	if err != nil {
		return 0, err
	}
	cutime, err := f(15)
	if err != nil {
		return 0, err
	}
	cstime, err := f(16)
	if err != nil {
		return 0, err
	}

	n += utime + stime + cutime + cstime
	return n, nil
}

func (c *CpuMonitor) Update() (self, box float64, dt time.Duration, e error) {
	idle, total, self0, err := getStats()
	if err != nil {
		return 0, 0, 0, err
	}

	idleTicks := float64(idle - c.idle)
	totalTicks := float64(total - c.total)
	selfTicks := float64(self0 - c.self)

	cpuUsage := (totalTicks - idleTicks) / totalTicks
	selfCpu := selfTicks / totalTicks

	n := float64(runtime.NumCPU())

	now := time.Now()

	dt = now.Sub(c.last)
	c.last = now

	c.total = total
	c.idle = idle
	c.self = self0

	return n * selfCpu, n * cpuUsage, dt, nil
}

type MemStats struct {
	Total    int `json:",omitempty"`
	Free     int `json:",omitempty"`
	SelfVirt int `json:",omitempty"`
	SelfRss  int `json:",omitempty"`
}

func (m MemStats) String() string {
	buf, _ := json.Marshal(m)
	return string(buf)
}

func LoadMem() (*MemStats, error) {
	out := &MemStats{}
	grab := func(file, field string) (int, error) {
		f, err := os.Open(file)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			line := s.Text()
			parts := strings.Fields(line)

			if parts[0] == field {
				if parts[2] != "kB" {
					return 0, fmt.Errorf("bad line: %q", line)
				}
				t, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					return 0, err
				}
				return 1024 * int(t), nil
			}
		}
		if err := s.Err(); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("couldn't find %q in %s", field, file)
	}
	if i, err := grab("/proc/meminfo", "MemTotal:"); err != nil {
		return nil, err
	} else {
		out.Total = i
	}
	if i, err := grab("/proc/meminfo", "MemFree:"); err != nil {
		return nil, err
	} else {
		out.Free = i
	}
	if i, err := grab("/proc/self/status", "VmSize:"); err != nil {
		return nil, err
	} else {
		out.SelfVirt = i
	}
	if i, err := grab("/proc/self/status", "VmRSS:"); err != nil {
		return nil, err
	} else {
		out.SelfRss = i
	}
	return out, nil
}
