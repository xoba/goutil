/*
modelling routines.

focused on gps algorithm from http://www-stat.stanford.edu/~jhf/ftp/GPSpub.pdf

*/
package model

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/xoba/goutil/gmath"
	"github.com/xoba/goutil/gmath/la"
	"github.com/xoba/goutil/gmath/stats"
	"math"
	"math/rand"
)

func (f *ObservationAssignments) isRowTraining(i int) int {
	for _, x := range f.TrainingIndicies {
		if x == i {
			return 1
		}
	}
	return 0
}

type RandomAssigner struct {
	N int     // total number of observations
	P float64 // probability that observation is a training example
}

func (x *RandomAssigner) Assign() *ObservationAssignments {
	var test, train []int
	for i := 0; i < x.N; i++ {
		if rand.Float64() <= x.P {
			train = append(train, i)
		} else {
			test = append(test, i)
		}
	}
	return &ObservationAssignments{
		TrainingIndicies: train,
		TestingIndicies:  test,
	}
}

type TrainingOnlyAssigner struct {
	N int
}

func (x *TrainingOnlyAssigner) Assign() *ObservationAssignments {
	var train []int
	for i := 0; i < x.N; i++ {
		train = append(train, i)
	}
	return &ObservationAssignments{
		TrainingIndicies: train,
	}
}

type PassiveMonitor func(v float64, jstar int, m []float64, trainingRisk, testRisk float64)

type FixedRoundsMonitor struct {
	Vmax  float64
	Debug PassiveMonitor
}

func (m *FixedRoundsMonitor) Continue(v float64, jstar int, a []float64, i, o float64, cn []Normalization, rn Normalization) bool {
	if m.Debug != nil {
		m.Debug(v, jstar, a, i, o)
	}
	return v < m.Vmax
}

type IntersectionMonitor struct {
	A Continue
	B PassiveMonitor
}

func (m *IntersectionMonitor) Continue(v float64, jstar int, a []float64, i, o float64, cn []Normalization, rn Normalization) bool {
	m.B(v, jstar, a, i, o)
	return m.A(v, jstar, a, i, o, cn, rn)
}

func RunGps(rp *RegressionProblem, testSet *ObservationAssignments, dv float64, mon Continue) *GpsResults {
	rc := &LinearRegressionRisk{}
	pc := NewLassoPenalty(len(rp.ColumnNames))
	return RunGpsFull(rp, testSet, dv, rc, pc, mon)
}

func RunGpsFull(rp *RegressionProblem, tt *ObservationAssignments, dv float64, rc RiskCalculator, pc PenaltyCalculator, proceed Continue) *GpsResults {

	check(ValidateRegressionProblem(rp))

	rowMask := func(indicies []int) []float64 {
		out := make([]float64, rp.Data.Rows)
		for _, x := range indicies {
			out[x] = 1.0
		}
		return out
	}

	trainMask := rowMask(tt.TrainingIndicies)
	testMask := rowMask(tt.TestingIndicies)

	cn := NormalizeColumnsInPlace(rp.Data, trainMask)
	rn := func() Normalization {
		switch rc.Norm() {
		case X_AND_Y:
			return NormalizeColumnsInPlace(rp.Response.AsColumnVector(), trainMask)[0]
		case X_ONLY:
			return Normalization{IsNone: true}
		default:
			panic("illegal norm type")
		}
	}()

	p := rp.P

	var v float64
	a := make([]float64, p)

	for {
		risk := rc.CalcRisk(a, trainMask, rp, CalcAll)
		dr := risk.Derivative

		penalty := pc.CalcPenalty(a, CalcAll)
		dp := penalty.Derivative

		lambda := NewKVList()
		S := NewKVList()

		for i := 0; i < p; i++ {
			li := -dr[i] / dp[i]
			kv := &KeyValue{i, math.Abs(li)}
			lambda.Add(kv)
			if li*a[i] < 0 {
				S.Add(kv)
			}
		}

		jstar := func() int {
			if S.Length == 0 {
				return lambda.Max.Index
			} else {
				return S.Max.Index
			}
		}()

		a[jstar] += dv * Sgn(-dr[jstar])
		v += dv

		testRisk := rc.CalcRisk(a, testMask, rp, ValueOnly).Value

		if !proceed(v, jstar, a, risk.Value, testRisk, cn, rn) {
			break
		}
	}

	var results GpsResults

	results.Response = SignalDimInfo{
		Name: rp.ResponseName,
		Norm: rn,
	}

	for i := 0; i < p; i++ {
		if a[i] != 0 {
			results.Predictors = append(results.Predictors, SignalDimInfo{
				Name:  rp.ColumnNames[i],
				Norm:  cn[i],
				Model: a[i],
			})
		}
	}

	return &results
}

type GpsResults struct {
	Predictors []SignalDimInfo
	Response   SignalDimInfo
}

func (i *GpsResults) String() string {
	return marshal(i)
}

func marshal(i interface{}) string {
	if buf, err := json.Marshal(i); err == nil {
		return string(buf)
	} else {
		return fmt.Sprintf("%v", i)
	}
}

type SignalDimInfo struct {
	Name  string
	Norm  Normalization
	Model float64 `json:",omitempty"`
}
type Normalization struct {
	Mean, Sd float64
	IsNone   bool `json:",omitempty"`
}

func (r *RegressionProblem) Copy() *RegressionProblem {
	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)
	d := gob.NewDecoder(&buf)
	check(e.Encode(r))
	var out RegressionProblem
	check(d.Decode(&out))
	return &out
}

type ValueAndDerivative struct {
	Value      float64
	Derivative []float64
}

func NormalizeColumnsInPlace(m *la.Matrix, rowMask []float64) (out []Normalization) {
	for j := 0; j < m.Cols; j++ {
		var list []float64
		for i := 0; i < m.Rows; i++ {
			if rowMask[i] == 1.0 {
				list = append(list, m.Get(i, j))
			}
		}
		norm := func() Normalization {
			mean, variance := stats.MeanVariance(list)
			if variance == 0 {
				variance = 1.0
			}
			return Normalization{
				Mean: mean,
				Sd:   math.Sqrt(variance),
			}
		}()
		out = append(out, norm)
		for i := 0; i < m.Rows; i++ {
			v := m.Get(i, j)
			v = (v - norm.Mean) / norm.Sd
			m.Set(i, j, v)
		}
	}
	return
}

type IndexSet struct {
	Length int
	Max    *KeyValue
}

func NewKVList() *IndexSet {
	return &IndexSet{
		Max: &KeyValue{-1, -math.MaxFloat64},
	}
}

type KeyValue struct {
	Index int
	Value float64
}

func (f *IndexSet) Add(kv *KeyValue) {
	if kv.Value > f.Max.Value {
		f.Max = kv
	}
	f.Length++
}

func Sgn(a float64) float64 {
	switch {
	case a < 0:
		return -1
	case a > 0:
		return +1
	}
	return 0
}

type MonitorFactory func() Continue

type CrossValResults struct {
	TestRisk   gmath.Function
	GpsResults *GpsResults
}

// return monitor for final gps run after cross-val
type FoldDecider func(oos gmath.Function) Continue

/*

runs a cross-validation

 folds: number of folds in the cross-validation
 rp:    the regression problem per se
 rc:    the risk measure
 pc:    penalty term
 mf:
 oa:    called each time to assign observations to folds
 fd:    decides how to regulate the final model run after cross-val
*/
func RunCrossVal(folds int, dv float64, rp *RegressionProblem, rc RiskCalculator, pc PenaltyCalculator, mf MonitorFactory, oa ObservationAssigner, fd FoldDecider) *CrossValResults {

	var oos gmath.Function

	for i := 0; i < folds; i++ {
		builder := gmath.NewInterpolationBuilder()
		tt := oa.Assign()
		mon := &IntersectionMonitor{
			A: mf(),
			B: func(v float64, jstar int, m []float64, trainingRisk, testRisk float64) {
				builder.Set(v, testRisk)
			},
		}
		RunGpsFull(rp, tt, dv, rc, pc, mon.Continue)

		f := builder.Init()

		if oos == nil {
			oos = f
		} else {
			oos = gmath.Add(oos, f, 100)
		}
	}

	oos = gmath.NewScaledFunction(1.0/float64(folds), oos)

	tt := &TrainingOnlyAssigner{rp.N}

	results := RunGpsFull(rp, tt.Assign(), dv, rc, pc, fd(oos))

	return &CrossValResults{
		TestRisk:   oos,
		GpsResults: results,
	}
}

type FixedVMonitor struct {
	Max float64
}

func (x *FixedVMonitor) Continue(v float64, jstar int, m []float64, i, o float64, cn []Normalization, rn Normalization) bool {
	return v < x.Max
}

func check(e interface{}) {
	if e != nil {
		panic(e)
	}
}
