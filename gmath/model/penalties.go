package model

import (
	"github.com/xoba/goutil/gmath/blas"
	"math"
)

type lassoPenalty struct {
	deriv []float64
}

func NewLassoPenalty(predictors int) *lassoPenalty {
	a := make([]float64, predictors)
	for i := 0; i < predictors; i++ {
		a[i] = 1
	}
	return &lassoPenalty{a}
}

func (lp *lassoPenalty) CalcPenalty(model []float64, a CalcAdvisor) ValueAndDerivative {
	var out ValueAndDerivative
	if !a.DontNeedValue {
		out.Value = blas.Dasum(len(model), model, 1)
	}
	if !a.DontNeedDerivative {
		out.Derivative = lp.deriv
	}
	return out
}

func NewRidgePenalty() *ridgePenalty {
	return &ridgePenalty{}
}

type ridgePenalty struct {
}

func (en *ridgePenalty) CalcPenalty(a []float64, advisor CalcAdvisor) ValueAndDerivative {
	n := len(a)
	x := make([]float64, n)
	var p float64
	for i := 0; i < n; i++ {
		abs := math.Abs(a[i])
		p += math.Pow(abs, 2)
		x[i] = 2 * abs
	}
	return ValueAndDerivative{
		Value:      p,
		Derivative: x,
	}
}

// elastic net for beta > 1 branch
type ElasticNet1Plus struct {
	Beta float64
}

// elastic net for beta < 1 branch
type ElasticNet1Minus struct {
	Beta float64
}

// create a new elastic net for 0<beta<=2
func NewElasticNetFamily(beta float64, n int) PenaltyCalculator {
	if beta <= 0 || beta > 2 {
		panic("bad beta")
	}
	switch {
	case beta < 0 || beta > 2:
		panic("bad beta")
	case beta == 2:
		return NewRidgePenalty()
	case beta > 1:
		return &ElasticNet1Plus{beta}
	case beta == 1:
		return NewLassoPenalty(n)
	case beta < 1:
		return &ElasticNet1Minus{beta}
	default:
		panic("illegal state")
	}
}
func (en *ElasticNet1Minus) CalcPenalty(a []float64, advisor CalcAdvisor) ValueAndDerivative {
	n := len(a)
	x := make([]float64, n)
	var p float64
	for i := 0; i < n; i++ {
		abs := math.Abs(a[i])
		arg := (1-en.Beta)*abs + en.Beta
		p += math.Log(arg)
		x[i] = (1 - en.Beta) / arg
	}
	return ValueAndDerivative{
		Value:      p,
		Derivative: x,
	}
}

func (en *ElasticNet1Plus) CalcPenalty(a []float64, advisor CalcAdvisor) ValueAndDerivative {
	n := len(a)
	x := make([]float64, n)
	var p float64
	for i := 0; i < n; i++ {
		abs := math.Abs(a[i])
		p += (en.Beta-1)*math.Pow(abs, 2)/2 + (2-en.Beta)*abs
		x[i] = 2*(en.Beta-1)*abs + 2 - en.Beta
	}
	return ValueAndDerivative{
		Value:      p,
		Derivative: x,
	}
}
