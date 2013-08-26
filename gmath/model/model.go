package model

import (
	"bytes"
	"encoding/gob"
	"github.com/xoba/goutil/gmath/blas"
	"github.com/xoba/goutil/gmath/la"
	"github.com/xoba/goutil/gmath/stats"
	"math"
	"math/rand"
)

type RegressionProblem struct {
	Data         *la.Matrix
	RowNames     []string
	ColumnNames  []string
	Response     *la.Vector
	ResponseName string
}

type ObservationAssignments struct {
	TrainingIndicies []int
	TestingIndicies  []int
}

func (f *ObservationAssignments) isRowTraining(i int) int {
	for _, x := range f.TrainingIndicies {
		if x == i {
			return 1
		}
	}
	return 0
}

type ObservationAssigner interface {
	Assign() *ObservationAssignments
}

type TrainingOnlyAssigner struct {
	n int
}

func (x *TrainingOnlyAssigner) Assign() *ObservationAssignments {
	var train []int
	for i := 0; i < x.n; i++ {
		train = append(train, i)
	}
	return &ObservationAssignments{
		TrainingIndicies: train,
	}
}

type RandomAssigner struct {
	n int
	f float64
}

func (x *RandomAssigner) Assign() *ObservationAssignments {
	var test, train []int
	for i := 0; i < x.n; i++ {
		if rand.Float64() <= x.f {
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

// monitors gps iterations
type Monitor interface {
	// whether or not to continue gps iterations
	Continue(v float64, jstar int, m []float64, trainingRisk, testRisk float64) bool
}

type PassiveMonitor func(v float64, jstar int, m []float64, trainingRisk, testRisk float64)

type DownRoundsMonitor struct {
	min       float64
	downCount int
	target    int
}

func NewDownRounds(target int) *DownRoundsMonitor {
	return &DownRoundsMonitor{target: target, min: math.MaxFloat64}
}

func (m *DownRoundsMonitor) Continue(v float64, jstar int, a []float64, i, o float64) bool {
	if o < m.min {
		m.downCount = 0
		m.min = o
	} else {
		m.downCount++
	}
	return m.downCount < m.target
}

type FixedRoundsMonitor struct {
	Vmax  float64
	Debug PassiveMonitor
}

func (m *FixedRoundsMonitor) Continue(v float64, jstar int, a []float64, i, o float64) bool {
	if m.Debug != nil {
		m.Debug(v, jstar, a, i, o)
	}
	return v < m.Vmax
}

type IntersectionMonitor struct {
	A Monitor
	B PassiveMonitor
}

func (m *IntersectionMonitor) Continue(v float64, jstar int, a []float64, i, o float64) bool {
	m.B(v, jstar, a, i, o)
	return m.A.Continue(v, jstar, a, i, o)
}

type RiskCalculator interface {
	// rowMask is comprised of 1.0 and 0.0 elements, to respectively mask in or out various rows
	CalcRisk(model []float64, rowMask []float64, rp *RegressionProblem, calcValue, calcDerivative bool) ValueAndDerivative
}

type PenaltyCalculator interface {
	CalcPenalty(model []float64, calcValue, calcDerivative bool) ValueAndDerivative
}

func RunGps(rp *RegressionProblem, testSet *ObservationAssignments, dv float64, mon Monitor) *GpsResults {
	rc := &LinearRegressionRisk{}
	pc := NewLassoPenalty(len(rp.ColumnNames))
	return RunGpsFull(rp, testSet, dv, rc, pc, mon)
}

func RunGpsFull(rp *RegressionProblem, tt *ObservationAssignments, dv float64, rc RiskCalculator, pc PenaltyCalculator, mon Monitor) *GpsResults {

	rowMask := func(indicies []int) []float64 {
		out := make([]float64, rp.Data.Rows)
		for _, x := range indicies {
			out[x] = 1.0
		}
		return out
	}

	trainMask := rowMask(tt.TrainingIndicies)
	testMask := rowMask(tt.TestingIndicies)

	cn := NormalizeColumns(rp.Data, trainMask)
	rn := NormalizeColumns(rp.Response.AsColumnVector(), trainMask)

	p := rp.NumPredictors()

	var v float64
	a := make([]float64, p)

	for {
		risk := rc.CalcRisk(a, trainMask, rp, true, true)
		dr := risk.Derivative

		penalty := pc.CalcPenalty(a, true, true)
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

		testRisk := rc.CalcRisk(a, testMask, rp, true, false).Value

		if !mon.Continue(v, jstar, a, risk.Value, testRisk) {
			break
		}
	}

	var results GpsResults

	results.Response = SignalDimInfo{
		Name: rp.ResponseName,
		Norm: rn[0],
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
type SignalDimInfo struct {
	Name  string
	Norm  Normalization
	Model float64 `json:",omitempty"`
}
type Normalization struct {
	Mean, Sd float64
}

func (r *RegressionProblem) NumObs() int {
	return r.Data.Rows
}
func (r *RegressionProblem) NumPredictors() int {
	return r.Data.Cols
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

type ElasticNet1 struct {
	Beta float64
}
type ElasticNet2 struct {
	Beta float64
}

func NewElasticNetFamily(beta float64, n int) PenaltyCalculator {
	if beta <= 0 || beta > 2 {
		panic("bad beta")
	}
	switch {
	case beta < 0 || beta > 2:
		panic("bad beta")
	case beta > 1:
		return &ElasticNet1{beta}
	case beta == 1:
		return NewLassoPenalty(n)
	case beta < 1:
		return &ElasticNet2{beta}
	default:
		panic("illegal state")
	}
}
func (en *ElasticNet2) CalcPenalty(a []float64, v, d bool) ValueAndDerivative {
	n := len(a)
	x := make([]float64, n)
	var p float64
	for i := 0; i < n; i++ {
		arg := (1-en.Beta)*math.Abs(a[i]) + en.Beta
		p += math.Log(arg)
		x[i] = (1 - en.Beta) / arg
	}
	return ValueAndDerivative{
		Value:      p,
		Derivative: x,
	}
}

func (en *ElasticNet1) CalcPenalty(a []float64, v, d bool) ValueAndDerivative {
	n := len(a)
	x := make([]float64, n)
	var p float64
	for i := 0; i < n; i++ {
		p += (en.Beta-1)*math.Pow(a[i], 2)/2 + (2-en.Beta)*math.Abs(a[i])
		x[i] = 2*(en.Beta-1)*math.Abs(a[i]) + 2 - en.Beta
	}
	return ValueAndDerivative{
		Value:      p,
		Derivative: x,
	}
}

type LassoPenalty struct {
	deriv []float64
}

func NewLassoPenalty(n int) *LassoPenalty {
	a := make([]float64, n)
	for i := 0; i < n; i++ {
		a[i] = 1
	}
	return &LassoPenalty{a}
}

func (lp *LassoPenalty) CalcPenalty(model []float64, v, d bool) ValueAndDerivative {
	var out ValueAndDerivative
	if v {
		out.Value = blas.Dasum(len(model), model, 1)
	}
	if d {
		out.Derivative = lp.deriv
	}
	return out
}

type LinearRegressionRisk struct {
}

func (lr LinearRegressionRisk) CalcRisk(model []float64, rowMask []float64, rp *RegressionProblem, calcValue, calcDerivative bool) ValueAndDerivative {

	n := rp.Data.Rows
	p := rp.NumPredictors()

	ea := make([]float64, n)
	blas.Dcopy(n, rp.Response.Elements, 1, ea, 1)

	{
		alpha := 1.0
		a := rp.Data
		x := model

		beta := -1.0
		y := ea

		blas.Dgemv("N", a.Rows, a.Cols, alpha, a.Elements, a.ColumnStride, x, 1, beta, y, 1)
	}

	tmp := make([]float64, n)
	blas.Dsbmv("L", n, 0, 1.0, rowMask, 1, ea, 1, 0, tmp, 1)
	ea = tmp

	var risk float64
	if calcValue {
		risk = math.Pow(blas.Dnrm2(len(ea), ea, 1), 2)
	}

	var g []float64
	if calcDerivative {
		g = make([]float64, p)

		alpha := 1.0
		a := rp.Data
		x := ea

		beta := 0.0
		y := g

		blas.Dgemv("T", a.Rows, a.Cols, alpha, a.Elements, a.ColumnStride, x, 1, beta, y, 1)
	}

	count := math.Pow(blas.Dnrm2(len(rowMask), rowMask, 1), 2)

	return ValueAndDerivative{
		Value:      risk / count,
		Derivative: g,
	}
}

type ValueAndDerivative struct {
	Value      float64
	Derivative []float64
}

func NormalizeColumns(m *la.Matrix, rowMask []float64) (out []Normalization) {
	for i := 0; i < m.Cols; i++ {
		var list []float64
		for j := 0; j < m.Rows; j++ {
			if rowMask[j] == 1.0 {
				list = append(list, m.Get(j, i))
			}
		}
		norm := func() Normalization {
			mean, variance := stats.MeanVariance(list)
			return Normalization{
				Mean: mean,
				Sd:   math.Sqrt(variance),
			}
		}()
		out = append(out, norm)
		for j := 0; j < m.Rows; j++ {
			v := m.Get(j, i)
			v = (v - norm.Mean) / norm.Sd
			if !(math.IsNaN(v) || math.IsInf(v, 0)) {
				m.Set(j, i, v)
			}
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
