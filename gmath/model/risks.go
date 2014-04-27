package model

import (
	"math"

	"github.com/xoba/goutil/gmath/blas"
)

type NormalizationType int

const (
	_ = iota
	X_AND_Y
	X_ONLY
)

type RiskCalculator interface {
	// rowMask is comprised of 1.0 and 0.0 elements, to respectively mask in or out various rows
	CalcRisk(model []float64, rowMask []float64, rp *RegressionProblem, a CalcAdvisor) ValueAndDerivative
	Norm() NormalizationType
}

type LogisticRegressionRisk struct {
}

// only normalize X matrix, since Y's are in {-1,1} set by definition
func (lr *LogisticRegressionRisk) Norm() NormalizationType {
	return X_ONLY
}

/*

Adapted from:

	public IRiskOutput calcRisk(double[] beta) {

		final int n = getN();
		final int p = getP();

		final double[][] x = ds.getPredictors();
		final double[] y = ds.getResponses();

		double risk = 0;
		final double[] g = new double[p];

		for (int i = 0; i < n; i++) {
			double f = 0;
			for (int j = 0; j < p; j++) {
				f += x[i][j] * beta[j];
			}
			double exp = Math.exp(-y[i] * f);
			double eyf = y[i] * exp / (1 + exp);

			risk += Math.log(1 + exp);
			for (int j = 0; j < p; j++) {
				g[j] += -eyf * x[i][j];
			}
		}

		final double finalRisk = risk / n;

		return new IRiskOutput() {

			@Override
			public double[] getRiskDerivative() {
				return g;
			}

			@Override
			public double getRisk() {
				return finalRisk;
			}
		};
	}

        // outputs the probability per observation
	public double[][] predict(double[] beta) {

		final int n = getN();
		final int p = getP();

		double[][] out = new double[n][1];

		final double[][] x = ds.getPredictors();

		for (int i = 0; i < n; i++) {
			double f = 0;
			for (int j = 0; j < p; j++) {
				f += x[i][j] * beta[j];
			}
			double e = Math.exp(f);
			out[i][0] = e / (1 + e);
		}

		return out;
	}


*/
func (lr *LogisticRegressionRisk) CalcRisk(beta []float64, rowMask []float64, rp *RegressionProblem, a CalcAdvisor) ValueAndDerivative {

	n := rp.N
	p := rp.P

	var risk float64
	g := make([]float64, p)

	for i := 0; i < n; i++ {
		if rowMask[i] == 0 {
			continue
		}
		f := 0.0
		for j := 0; j < p; j++ {
			f += rp.Data.Get(i, j) * beta[j]
		}
		y := rp.Response.Get(i)
		exp := math.Exp(-y * f)
		eyf := y * exp / (1 + exp)

		risk += math.Log(1 + exp)

		for j := 0; j < p; j++ {
			g[j] += -eyf * rp.Data.Get(i, j)
		}
	}

	return ValueAndDerivative{
		Value:      risk,
		Derivative: g,
	}
}

type LinearRegressionRisk struct {
}

func (lr *LinearRegressionRisk) Norm() NormalizationType {
	return X_AND_Y
}

func (lr *LinearRegressionRisk) CalcRisk(model []float64, rowMask []float64, rp *RegressionProblem, a CalcAdvisor) ValueAndDerivative {

	n := rp.N
	p := rp.P

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
	if !a.DontNeedValue {
		risk = math.Pow(blas.Dnrm2(len(ea), ea, 1), 2)
	}

	var g []float64
	if !a.DontNeedDerivative {
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
