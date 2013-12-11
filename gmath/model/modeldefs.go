package model

import (
	"fmt"
	"github.com/xoba/goutil/gmath/la"
)

// the data and metadata for a regression problem
type RegressionProblem struct {
	N, P         int        // number of observations and predictors, respectively
	Data         *la.Matrix // i.e., the NxP design matrix
	RowNames     []string   // N-dim slice of row names
	ColumnNames  []string
	Response     *la.Vector
	ResponseName string
}

// make sure regression problem is self-consistent
func ValidateRegressionProblem(rp *RegressionProblem) error {

	var errors []error

	item := func(t string, i, j int) {
		if i != j {
			e := fmt.Errorf("%d vs %d %s", i, j, t)
			errors = append(errors, e)
		}
	}

	item("rows", rp.Data.Rows, rp.N)
	item("cols", rp.Data.Cols, rp.P)
	item("row names", len(rp.RowNames), rp.N)
	item("column names", len(rp.ColumnNames), rp.P)
	item("responses", rp.Response.Size, rp.N)

	if len(errors) == 0 {
		return nil
	} else {
		return fmt.Errorf("invalid regression problem: %v", errors)
	}
}

type ObservationAssignments struct {
	TrainingIndicies []int
	TestingIndicies  []int
}

type ObservationAssigner interface {
	Assign() *ObservationAssignments
}

// monitors gps iterations, decided whether or not to continue
type Continue func(v float64, jstar int, m []float64, trainingRisk, testRisk float64) bool

// advisory flags as to whether we need various components
type CalcAdvisor struct {
	DontNeedValue      bool
	DontNeedDerivative bool
}

var (
	ValueOnly CalcAdvisor = CalcAdvisor{DontNeedValue: false, DontNeedDerivative: true}
	DerivOnly CalcAdvisor = CalcAdvisor{DontNeedValue: true, DontNeedDerivative: false}
	CalcAll   CalcAdvisor = CalcAdvisor{}
)

// calculate a model penalty and its derivative; note that derivative is respect to absolute values of model coefficients
type PenaltyCalculator interface {
	CalcPenalty(model []float64, a CalcAdvisor) ValueAndDerivative
}
