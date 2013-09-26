// use modelling code on well-known prostate cancer data, check we get same results.
package cancer

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"github.com/xoba/goutil/gmath"
	"github.com/xoba/goutil/gmath/model"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Tool struct {
}

func (*Tool) Name() string {
	return "cancer,run the standard cancer model"
}

type mon struct {
	maxV     float64
	csv      string
	rp       *model.RegressionProblem
	f        *os.File
	min, max float64
	oos      gmath.Function
}

func newMon(maxV float64, rp *model.RegressionProblem) *mon {
	csv := "/tmp/" + uuid.New() + ".csv"

	out := &mon{
		maxV: maxV,
		csv:  csv,
		rp:   rp,
		min:  math.MaxFloat64,
		max:  -math.MaxFloat64,
	}

	f, err := os.Create(csv)
	check(err)
	out.f = f
	fmt.Fprintf(f, "v\tjstar\toos\t%s\n", strings.Join(rp.ColumnNames, "\t"))

	return out
}

func (x *mon) Continue(v float64, jstar int, m []float64, i, o float64) bool {
	fmt.Fprintf(x.f, "%f\t%d\t%f", v, jstar, x.oos.Eval(v))
	for _, y := range m {
		fmt.Fprintf(x.f, "\t%f", y)
		if y < x.min {
			x.min = y
		}
		if y > x.max {
			x.max = y
		}
	}
	fmt.Fprintln(x.f)
	return v < x.maxV
}

func (m *mon) Close() error {
	return m.f.Close()
}

func (*Tool) Run(args []string) {
	rp, _ := Load()
	RunCancerCrossVal(rp)
}

func RunCancerCrossVal(rp *model.RegressionProblem) {

	vmax := 1.65

	rc := &model.LinearRegressionRisk{}

	pc := func() model.PenaltyCalculator {
		if true {
			return model.NewElasticNetFamily(1, len(rp.ColumnNames))
		} else {
			return model.NewLassoPenalty(len(rp.ColumnNames))
		}
	}()

	oa := &model.RandomAssigner{rp.Data.Rows, 2.0 / 3.0}

	mf := func() model.Monitor {
		return &model.FixedVMonitor{vmax}
	}

	m := newMon(vmax, rp)
	defer os.Remove(m.csv)

	var bestV float64

	fd := func(oos gmath.Function) model.Monitor {

		m.oos = oos

		bestV = func() float64 {
			x0, x1 := oos.Domain()
			min := x0
			di := 0.03
			for i := x0; i < x1; i += di {
				if v := oos.Eval(i); v < oos.Eval(min) {
					min = i
				}
			}
			// now back off up to 5% worse
			tol := 1.05 * oos.Eval(min)
			for i := min; i >= x0; i -= di {
				if v := oos.Eval(i); v < tol {
					min = i
				}
			}
			return min
		}()

		return m
	}

	cv := model.RunCrossVal(10, 0.001, rp, rc, pc, mf, oa, fd)
	fmt.Printf("%v\n", cv.GpsResults)

	m.Close()

	r := "/tmp/" + uuid.New() + ".r"
	defer os.Remove(r)

	offset := 0.0
	factor := 0.5

	ioutil.WriteFile(r, []byte(rscript(time.Now().UTC().Format("2006-01-02T15:04:05Z"), m.csv, bestV, m.min, m.max, offset, factor)), os.ModePerm)

	sh := "/tmp/" + uuid.New() + ".sh"
	defer os.Remove(sh)
	ioutil.WriteFile(sh, []byte(shellScript(r)), os.ModePerm)

	os.Chmod(sh, 0777)

	check(runCommand(sh))

	cmd := exec.Command("evince", "Rplots.pdf")
	cmd.Start()

}

func shellScript(fn string) string {
	return fmt.Sprintf(`#!/bin/bash
R --vanilla < %s
`, fn)
}

func runCommand(c string) error {
	parts := strings.Split(c, " ")
	cmd := exec.Command(parts[0], parts[1:]...)
	return cmd.Run()
}

func rscript(subtitle string, fn string, abline, y0, y1, oosOffset, oosFactor float64) string {
	return fmt.Sprintf(`dat=read.csv(file='%s',sep='\t')

plot(main='Prostate cancer model\n(%s)',
xlab='regularizer',ylab='coefficients',
dat$v,dat$lcavol,type='l',
ylim=c(%f,%f))
lines(dat$v,dat$svi,col='red')
lines(dat$v,dat$lweight,col='green')

lines(dat$v,dat$age,col='blue')
lines(dat$v,dat$lbph,col='green')
lines(dat$v,dat$lcp,col='orange')
lines(dat$v,dat$gleason,col='darkred')
lines(dat$v,dat$pgg45,col='purple')

lines(dat$v, %f + %f * dat$oos,lty=2)

points(dat$v,-0.05-dat$jstar/100,col='grey',pch='.')

f = 0.85
n = round(f*length(dat$v))

text(dat$v[n],dat$lcavol[n],"2")
text(dat$v[n],dat$lweight[n],"3")
text(dat$v[n],dat$age[n],"4")
text(dat$v[n],dat$lbph[n],"5")
text(dat$v[n],dat$svi[n],"6")
text(dat$v[n],dat$lcp[n],"7")
text(dat$v[n],dat$gleason[n],"8")
text(dat$v[n],dat$pgg45[n],"9")
	
grid()

abline(v=%f,lty=2)
`, fn, subtitle, y0, y1, oosOffset, oosFactor, abline)
}

func check(e interface{}) {
	if e != nil {
		panic(e)
	}
}
