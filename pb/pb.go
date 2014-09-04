// parallel boosting
package pb

import (
	"fmt"
	"math"
)

type Features []float64

type Problem struct {
	Observations []Features
	Responses    []float64
}

func Iterate(pr Problem, weights []float64, lr float64) error {
	n := len(pr.Observations)
	p := len(weights)
	q := make([]float64, n)
	for i := 0; i < n; i++ {
		var f float64
		for j := 0; j < p; j++ {
			f += weights[j] * pr.Observations[i][j]
		}
		q[i] = 1 / (1 + math.Exp(float64(pr.Responses[i])*f))
	}
	var up, um float64
	var errs []error
	for j := 0; j < p; j++ {
		for i := 0; i < n; i++ {
			if float64(pr.Responses[i])*pr.Observations[i][j] > 0 {
				up += q[i] * math.Abs(pr.Observations[i][j])
			} else {
				um += q[i] * math.Abs(pr.Observations[i][j])
			}
		}
		d := math.Log(up / um)
		if !(math.IsNaN(d) || math.IsInf(d, 0)) {
			weights[j] += lr * d
		} else {
			errs = append(errs, fmt.Errorf("d[%d]=%f", j, d))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors: %v", errs)
	}
	return nil
}

func LogisticRisk(pr Problem, weights []float64) float64 {
	var out float64
	n := len(pr.Observations)
	p := len(weights)
	for i := 0; i < n; i++ {
		y := float64(pr.Responses[i])
		var f float64
		for j := 0; j < p; j++ {
			f += weights[j] * pr.Observations[i][j]
		}
		out += math.Log(1 + math.Exp(-y*f))
	}
	return out / float64(n)
}
