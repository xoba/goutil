// logistic modelling example.
package logistic

import (
	"fmt"
	"github.com/xoba/goutil/gmath/la"
	"github.com/xoba/goutil/gmath/model"
	"math/rand"
)

type Tool struct {
}

func (*Tool) Name() string {
	return "logistic,modelling example"
}

func (*Tool) Run(args []string) {
	fmt.Println("running logistic regression")

	n := 1000
	p := 10

	beta := make([]float64, p)
	beta[0] = rand.NormFloat64()
	beta[1] = rand.NormFloat64()

	x := la.NewMatrix(n, p)
	y := la.NewVector(n)

	for i := 0; i < n; i++ {

		v := randVec(p)

		var z float64

		for j := 0; j < p; j++ {
			x.Set(i, j, v[j])
			z += beta[j]
		}

		if z > 0 {
			y.Set(i, +1)
		} else {
			y.Set(i, -1)
		}
	}

	rp := &model.RegressionProblem{
		N:            n,
		P:            p,
		Data:         x,
		Response:     y,
		ColumnNames:  names("p", p),
		RowNames:     names("x", n),
		ResponseName: "y",
	}

	rc := &model.LogisticRegressionRisk{}
	pc := model.NewLassoPenalty(p)

	dv := 0.001
	vmax := 0.07

	mon := &FixedVMonitor{vmax}

	oa := &model.RandomAssigner{rp.Data.Rows, 2.0 / 3.0}

	tt := oa.Assign()

	results := model.RunGpsFull(rp, tt, dv, rc, pc, mon)

	fmt.Println(results)

}

type FixedVMonitor struct {
	Max float64
}

func (x *FixedVMonitor) Continue(v float64, jstar int, m []float64, i, o float64) bool {
	fmt.Printf("%f\t%d\t%f\t%f\n", v, jstar, i, o)
	return v < x.Max
}

func randVec(p int) (out []float64) {
	for j := 0; j < p; j++ {
		out = append(out, rand.NormFloat64())
	}
	return
}

func names(m string, n int) (out []string) {
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("%s-%d", m, i))
	}
	return
}
