package model

import (
	"bufio"
	"code.google.com/p/go-uuid/uuid"
	"encoding/json"
	"fmt"
	"github.com/xoba/goutil/gmath"
	"github.com/xoba/goutil/gmath/la"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type Tool struct {
}

func (t *Tool) Tags() []string {
	return []string{}
}
func (m *Tool) Name() string {
	return "cancer"
}
func (m *Tool) Description() string {
	return "play with cancer data"
}

type mon struct {
	csv      string
	rp       *RegressionProblem
	f        *os.File
	min, max float64
	v        []float64
	oos      *gmath.InterpolationBuilder
	other    gmath.Function
	write    bool
}

func NewMon(rp *RegressionProblem, write bool) *mon {
	csv := "/tmp/" + uuid.New() + ".csv"

	out := &mon{
		csv:   csv,
		rp:    rp,
		min:   math.MaxFloat64,
		max:   -math.MaxFloat64,
		oos:   gmath.NewInterpolationBuilder(),
		write: write,
	}

	if write {
		f, err := os.Create(csv)
		check(err)
		out.f = f
		fmt.Fprintf(f, "v\tjstar\toos\tother\t%s\n", strings.Join(rp.ColumnNames, "\t"))
	}

	return out
}

func (m *mon) Close() error {
	if m.write {
		return m.f.Close()
	} else {
		return nil
	}
}

func (x *mon) Continue(v float64, jstar int, m []float64, i, o float64) bool {
	x.oos.Set(v, o)
	x.v = append(x.v, v)
	var z float64
	if x.other != nil {
		z = x.other.Eval(v)
	}
	if x.write {
		fmt.Fprintf(x.f, "%f\t%d\t%f\t%f", v, jstar, o, z)
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
	}
	return v < 1.7
}

func (*Tool) Run(args []string) {
	rp, _ := LoadTestCancerData()
	RunCancerCrossVal(rp)
}

func RunCancerCrossVal(rp *RegressionProblem) {

	oa := &RandomAssigner{n: rp.Data.Rows, f: 2.0 / 3.0}

	var oos gmath.Function

	rc := &LinearRegressionRisk{}
	pc := func() PenaltyCalculator {
		if true {
			return NewElasticNetFamily(1.0, len(rp.ColumnNames))
		} else {
			return NewLassoPenalty(len(rp.ColumnNames))
		}
	}()

	iter := 10
	for i := 0; i < iter; i++ {
		fmt.Printf("round %d\n", i)
		tt := oa.Assign()
		m := NewMon(rp, false)
		defer os.Remove(m.csv)
		RunGpsFull(rp, tt, 0.001, rc, pc, m)
		m.Close()
		if oos == nil {
			oos = m.oos.Init()
		} else {
			oos = gmath.Add(oos, m.oos.Init(), 100)
		}
	}

	m := NewMon(rp, true)
	defer os.Remove(m.csv)

	m.other = gmath.NewScaledFunction(1.0/float64(iter), oos)

	fmt.Println(m.other.Domain())
	fmt.Println(m.other.Range())
	bestV := func() float64 {
		x0, x1 := m.other.Domain()
		min := x0
		di := 0.03
		for i := x0; i < x1; i += di {
			if v := m.other.Eval(i); v < m.other.Eval(min) {
				min = i
			}
		}
		// now back off up to 5% worse
		tol := 1.05 * m.other.Eval(min)
		for i := min; i >= x0; i -= di {
			if v := m.other.Eval(i); v < tol {
				min = i
			}
		}
		return min
	}()

	start := time.Now()
	results := RunGpsFull(rp, (&TrainingOnlyAssigner{rp.Data.Rows}).Assign(), 0.001, rc, pc, m)
	dur := time.Now().Sub(start)
	m.Close()

	fmt.Printf("model time: %v\n", dur)

	if buf, err := json.Marshal(results); err == nil {
		fmt.Println(string(buf))
	}

	r := "/tmp/" + uuid.New() + ".r"
	defer os.Remove(r)
	ioutil.WriteFile(r, []byte(Rscript(time.Now().Format("20060102T150405"), m.csv, bestV, m.min, m.max)), os.ModePerm)

	sh := "/tmp/" + uuid.New() + ".sh"
	defer os.Remove(sh)
	ioutil.WriteFile(sh, []byte(ShellScript(r)), os.ModePerm)

	os.Chmod(sh, 0777)

	check(run(sh))

	cmd := exec.Command("evince", "Rplots.pdf")
	cmd.Start()

}

type MonitorFactory func(crossval bool) Monitor

func RunCrossVal(folds int, rp *RegressionProblem, rc RiskCalculator, pc PenaltyCalculator, mf MonitorFactory, oa ObservationAssigner) {

	var oos gmath.Function

	for i := 0; i < folds; i++ {
		fmt.Printf("round %d\n", i)
		builder := gmath.NewInterpolationBuilder()
		tt := oa.Assign()
		m := &IntersectionMonitor{
			A: mf(true),
			B: func(v float64, jstar int, m []float64, trainingRisk, testRisk float64) {
				fmt.Printf("%d %20.6e %20.6e %20.6e\n", i, v, trainingRisk, testRisk)
				builder.Set(v, testRisk)
			},
		}

		RunGpsFull(rp, tt, 0.001, rc, pc, m)
		if oos == nil {
			oos = builder.Init()
		} else {
			oos = gmath.Add(oos, builder.Init(), 100)
		}
	}

	oos = gmath.NewScaledFunction(1.0/float64(folds), oos)

	fmt.Println(oos.Domain())
	fmt.Println(oos.Range())

	func() {
		f, err := os.Create("oos.csv")
		check(err)
		defer f.Close()
		x0, x1 := oos.Domain()
		fmt.Fprintln(f, "v,r")
		for x := x0; x <= x1; x += (x1 - x0) / 100 {
			fmt.Fprintf(f, "%f,%f\n", x, oos.Eval(x))
		}
	}()

	bestV := func() float64 {
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

	fmt.Printf("best v = %f\n", bestV)

}

func ShellScript(fn string) string {
	return fmt.Sprintf(`#!/bin/bash
R --vanilla < %s
`, fn)
}

func run(c string) error {
	parts := strings.Split(c, " ")
	cmd := exec.Command(parts[0], parts[1:]...)
	return cmd.Run()
}

func LoadTestCancerData() (*RegressionProblem, *ObservationAssignments) {
	scanner := bufio.NewScanner(strings.NewReader(CSV))
	var lines [][]string
	for scanner.Scan() {
		lines = append(lines, strings.Split(scanner.Text(), "\t"))
	}
	if scanner.Err() != nil {
		panic("can't read data")
	}

	var test, train []int

	var rowNames []string

	header := lines[0]
	x := la.New(len(lines)-1, 8)
	for i := 0; i < x.Rows; i++ {
		for j := 0; j < x.Cols; j++ {
			if f, err := strconv.ParseFloat(strings.TrimSpace(lines[i+1][j+1]), 64); err == nil {
				x.Set(i, j, f)
			}
		}

		switch lines[i+1][10] {
		case "T":
			train = append(train, i)
		case "F":
			test = append(test, i)
		}

		rowNames = append(rowNames, lines[i+1][0])
	}

	y := la.NewVector(len(lines) - 1)
	for i := 0; i < y.Size; i++ {
		if f, err := strconv.ParseFloat(strings.TrimSpace(lines[i+1][9]), 64); err == nil {
			y.Set(i, f)
		}
	}

	out := RegressionProblem{
		Data:         x,
		Response:     y,
		ColumnNames:  header[1 : len(header)-2],
		RowNames:     rowNames,
		ResponseName: header[9],
	}

	tt := ObservationAssignments{
		TrainingIndicies: train,
		TestingIndicies:  test,
	}

	return &out, &tt
}

func Rscript(subtitle string, fn string, abline, y0, y1 float64) string {
	return fmt.Sprintf(`dat=read.csv(file='%s',sep='\t')

plot(main='Cancer model\n(%s)',
xlab='regularizer',ylab='coefficients',
dat$v,dat$lcavol,type='l',
ylim=c(%f,%f))
lines(dat$v,dat$svi,col='red')
lines(dat$v,dat$lweight,col='green')

lines(dat$v,dat$age,col='blue')
lines(dat$v,dat$lbph,col='green')
lines(dat$v,dat$lcp,col='orange')
lines(dat$v,dat$gleason,col='grey')
lines(dat$v,dat$pgg45,col='purple')

lines(dat$v,dat$other/2,col='grey',lty=4)
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
`, fn, subtitle, y0, y1, abline)
}

// CREDITS: Stamey, T.A., Kabalin, J.N., McNeal, J.E.,
// Johnstone, I.M., Freiha, F., Redwine, E.A. and Yang, N. (1989)
// Prostate specific antigen in the diagnosis and treatment of
// adenocarcinoma of the prostate: II. radical prostatectomy
// treated patients, Journal of Urology 141(5), 1076–1083.

const CSV = `	lcavol	lweight	age	lbph	svi	lcp	gleason	pgg45	lpsa	train
1	-0.579818495	2.769459	50	-1.38629436	0	-1.38629436	6	  0	-0.4307829	T
2	-0.994252273	3.319626	58	-1.38629436	0	-1.38629436	6	  0	-0.1625189	T
3	-0.510825624	2.691243	74	-1.38629436	0	-1.38629436	7	 20	-0.1625189	T
4	-1.203972804	3.282789	58	-1.38629436	0	-1.38629436	6	  0	-0.1625189	T
5	 0.751416089	3.432373	62	-1.38629436	0	-1.38629436	6	  0	 0.3715636	T
6	-1.049822124	3.228826	50	-1.38629436	0	-1.38629436	6	  0	 0.7654678	T
7	 0.737164066	3.473518	64	 0.61518564	0	-1.38629436	6	  0	 0.7654678	F
8	 0.693147181	3.539509	58	 1.53686722	0	-1.38629436	6	  0	 0.8544153	T
9	-0.776528789	3.539509	47	-1.38629436	0	-1.38629436	6	  0	 1.0473190	F
10	 0.223143551	3.244544	63	-1.38629436	0	-1.38629436	6	  0	 1.0473190	F
11	 0.254642218	3.604138	65	-1.38629436	0	-1.38629436	6	  0	 1.2669476	T
12	-1.347073648	3.598681	63	 1.26694760	0	-1.38629436	6	  0	 1.2669476	T
13	 1.613429934	3.022861	63	-1.38629436	0	-0.59783700	7	 30	 1.2669476	T
14	 1.477048724	2.998229	67	-1.38629436	0	-1.38629436	7	  5	 1.3480731	T
15	 1.205970807	3.442019	57	-1.38629436	0	-0.43078292	7	  5	 1.3987169	F
16	 1.541159072	3.061052	66	-1.38629436	0	-1.38629436	6	  0	 1.4469190	T
17	-0.415515444	3.516013	70	 1.24415459	0	-0.59783700	7	 30	 1.4701758	T
18	 2.288486169	3.649359	66	-1.38629436	0	 0.37156356	6	  0	 1.4929041	T
19	-0.562118918	3.267666	41	-1.38629436	0	-1.38629436	6	  0	 1.5581446	T
20	 0.182321557	3.825375	70	 1.65822808	0	-1.38629436	6	  0	 1.5993876	T
21	 1.147402453	3.419365	59	-1.38629436	0	-1.38629436	6	  0	 1.6389967	T
22	 2.059238834	3.501043	60	 1.47476301	0	 1.34807315	7	 20	 1.6582281	F
23	-0.544727175	3.375880	59	-0.79850770	0	-1.38629436	6	  0	 1.6956156	T
24	 1.781709133	3.451574	63	 0.43825493	0	 1.17865500	7	 60	 1.7137979	T
25	 0.385262401	3.667400	69	 1.59938758	0	-1.38629436	6	  0	 1.7316555	F
26	 1.446918983	3.124565	68	 0.30010459	0	-1.38629436	6	  0	 1.7664417	F
27	 0.512823626	3.719651	65	-1.38629436	0	-0.79850770	7	 70	 1.8000583	T
28	-0.400477567	3.865979	67	 1.81645208	0	-1.38629436	7	 20	 1.8164521	F
29	 1.040276712	3.128951	67	 0.22314355	0	 0.04879016	7	 80	 1.8484548	T
30	 2.409644165	3.375880	65	-1.38629436	0	 1.61938824	6	  0	 1.8946169	T
31	 0.285178942	4.090169	65	 1.96290773	0	-0.79850770	6	  0	 1.9242487	T
32	 0.182321557	3.804438	65	 1.70474809	0	-1.38629436	6	  0	 2.0082140	F
33	 1.275362800	3.037354	71	 1.26694760	0	-1.38629436	6	  0	 2.0082140	T
34	 0.009950331	3.267666	54	-1.38629436	0	-1.38629436	6	  0	 2.0215476	F
35	-0.010050336	3.216874	63	-1.38629436	0	-0.79850770	6	  0	 2.0476928	T
36	 1.308332820	4.119850	64	 2.17133681	0	-1.38629436	7	  5	 2.0856721	F
37	 1.423108334	3.657131	73	-0.57981850	0	 1.65822808	8	 15	 2.1575593	T
38	 0.457424847	2.374906	64	-1.38629436	0	-1.38629436	7	 15	 2.1916535	T
39	 2.660958594	4.085136	68	 1.37371558	1	 1.83258146	7	 35	 2.2137539	T
40	 0.797507196	3.013081	56	 0.93609336	0	-0.16251893	7	  5	 2.2772673	T
41	 0.620576488	3.141995	60	-1.38629436	0	-1.38629436	9	 80	 2.2975726	T
42	 1.442201993	3.682610	68	-1.38629436	0	-1.38629436	7	 10	 2.3075726	F
43	 0.582215620	3.865979	62	 1.71379793	0	-0.43078292	6	  0	 2.3272777	T
44	 1.771556762	3.896909	61	-1.38629436	0	 0.81093022	7	  6	 2.3749058	F
45	 1.486139696	3.409496	66	 1.74919985	0	-0.43078292	7	 20	 2.5217206	T
46	 1.663926098	3.392829	61	 0.61518564	0	-1.38629436	7	 15	 2.5533438	T
47	 2.727852828	3.995445	79	 1.87946505	1	 2.65675691	9	100	 2.5687881	T
48	 1.163150810	4.035125	68	 1.71379793	0	-0.43078292	7	 40	 2.5687881	F
49	 1.745715531	3.498022	43	-1.38629436	0	-1.38629436	6	  0	 2.5915164	F
50	 1.220829921	3.568123	70	 1.37371558	0	-0.79850770	6	  0	 2.5915164	F
51	 1.091923301	3.993603	68	-1.38629436	0	-1.38629436	7	 50	 2.6567569	T
52	 1.660131027	4.234831	64	 2.07317193	0	-1.38629436	6	  0	 2.6775910	T
53	 0.512823626	3.633631	64	 1.49290410	0	 0.04879016	7	 70	 2.6844403	F
54	 2.127040520	4.121473	68	 1.76644166	0	 1.44691898	7	 40	 2.6912431	F
55	 3.153590358	3.516013	59	-1.38629436	0	-1.38629436	7	  5	 2.7047113	F
56	 1.266947603	4.280132	66	 2.12226154	0	-1.38629436	7	 15	 2.7180005	T
57	 0.974559640	2.865054	47	-1.38629436	0	 0.50077529	7	  4	 2.7880929	F
58	 0.463734016	3.764682	49	 1.42310833	0	-1.38629436	6	  0	 2.7942279	T
59	 0.542324291	4.178226	70	 0.43825493	0	-1.38629436	7	 20	 2.8063861	T
60	 1.061256502	3.851211	61	 1.29472717	0	-1.38629436	7	 40	 2.8124102	T
61	 0.457424847	4.524502	73	 2.32630162	0	-1.38629436	6	  0	 2.8419982	T
62	 1.997417706	3.719651	63	 1.61938824	1	 1.90954250	7	 40	 2.8535925	F
63	 2.775708850	3.524889	72	-1.38629436	0	 1.55814462	9	 95	 2.8535925	T
64	 2.034705648	3.917011	66	 2.00821403	1	 2.11021320	7	 60	 2.8820035	F
65	 2.073171929	3.623007	64	-1.38629436	0	-1.38629436	6	  0	 2.8820035	F
66	 1.458615023	3.836221	61	 1.32175584	0	-0.43078292	7	 20	 2.8875901	F
67	 2.022871190	3.878466	68	 1.78339122	0	 1.32175584	7	 70	 2.9204698	T
68	 2.198335072	4.050915	72	 2.30757263	0	-0.43078292	7	 10	 2.9626924	T
69	-0.446287103	4.408547	69	-1.38629436	0	-1.38629436	6	  0	 2.9626924	T
70	 1.193922468	4.780383	72	 2.32630162	0	-0.79850770	7	  5	 2.9729753	T
71	 1.864080131	3.593194	60	-1.38629436	1	 1.32175584	7	 60	 3.0130809	T
72	 1.160020917	3.341093	77	 1.74919985	0	-1.38629436	7	 25	 3.0373539	T
73	 1.214912744	3.825375	69	-1.38629436	1	 0.22314355	7	 20	 3.0563569	F
74	 1.838961071	3.236716	60	 0.43825493	1	 1.17865500	9	 90	 3.0750055	F
75	 2.999226163	3.849083	69	-1.38629436	1	 1.90954250	7	 20	 3.2752562	T
76	 3.141130476	3.263849	68	-0.05129329	1	 2.42036813	7	 50	 3.3375474	T
77	 2.010894999	4.433789	72	 2.12226154	0	 0.50077529	7	 60	 3.3928291	T
78	 2.537657215	4.354784	78	 2.32630162	0	-1.38629436	7	 10	 3.4355988	T
79	 2.648300197	3.582129	69	-1.38629436	1	 2.58399755	7	 70	 3.4578927	T
80	 2.779440197	3.823192	63	-1.38629436	0	 0.37156356	7	 50	 3.5130369	F
81	 1.467874348	3.070376	66	 0.55961579	0	 0.22314355	7	 40	 3.5160131	T
82	 2.513656063	3.473518	57	 0.43825493	0	 2.32727771	7	 60	 3.5307626	T
83	 2.613006652	3.888754	77	-0.52763274	1	 0.55961579	7	 30	 3.5652984	T
84	 2.677590994	3.838376	65	 1.11514159	0	 1.74919985	9	 70	 3.5709402	F
85	 1.562346305	3.709907	60	 1.69561561	0	 0.81093022	7	 30	 3.5876769	T
86	 3.302849259	3.518980	64	-1.38629436	1	 2.32727771	7	 60	 3.6309855	T
87	 2.024193067	3.731699	58	 1.63899671	0	-1.38629436	6	  0	 3.6800909	T
88	 1.731655545	3.369018	62	-1.38629436	1	 0.30010459	7	 30	 3.7123518	T
89	 2.807593831	4.718052	65	-1.38629436	1	 2.46385324	7	 60	 3.9843437	T
90	 1.562346305	3.695110	76	 0.93609336	1	 0.81093022	7	 75	 3.9936030	T
91	 3.246490992	4.101817	68	-1.38629436	0	-1.38629436	6	  0	 4.0298060	T
92	 2.532902848	3.677566	61	 1.34807315	1	-1.38629436	7	 15	 4.1295508	T
93	 2.830267834	3.876396	68	-1.38629436	1	 1.32175584	7	 60	 4.3851468	T
94	 3.821003607	3.896909	44	-1.38629436	1	 2.16905370	7	 40	 4.6844434	T
95	 2.907447359	3.396185	52	-1.38629436	1	 2.46385324	7	 10	 5.1431245	F
96	 2.882563575	3.773910	68	 1.55814462	1	 1.55814462	7	 80	 5.4775090	T
97	 3.471966453	3.974998	68	 0.43825493	1	 2.90416508	7	 20	 5.5829322	F
`

func check(e interface{}) {
	if e != nil {
		panic(e)
	}
}
