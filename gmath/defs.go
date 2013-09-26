// math code.
package gmath

import (
	"fmt"
	"math"
	"sort"
)

type Tool struct {
}

func (*Tool) Name() string {
	return "math"
}
func (*Tool) Description() string {
	return "play with math stuff"
}

// play with math stuff
func (*Tool) Run(args []string) {
	fb := NewInterpolationBuilder()
	for i := 0; i < 10; i++ {
		v := float64(i)
		fb.Set(v, math.Pow(v, 2))
	}
	fb.Set(float64(3), 9.0) // test effect of duplicate value
	f := fb.Init()
	fmt.Println(f.Domain())
	fmt.Println(f.Range())
	fmt.Printf("f(%f) = %f\n", 0.0, f.Eval(0))
	for x := -1.0; x < 12; x += 0.2 {
		fmt.Printf("f(%f) = %f\n", x, f.Eval(x))
	}
}

func Db(v float64) float64 {
	return 10 * math.Log10(v)
}
func Idb(v float64) float64 {
	return math.Pow(10, v/10)
}

type InterpolationBuilder struct {
	min, max float64
	values   []ival
}

func NewInterpolationBuilder() *InterpolationBuilder {
	return &InterpolationBuilder{min: math.MaxFloat64, max: -math.MaxFloat64}
}

func (f *InterpolationBuilder) Len() int {
	return len(f.values)
}
func (f *InterpolationBuilder) Less(i, j int) bool {
	return f.values[i].x < f.values[j].x
}
func (f *InterpolationBuilder) Swap(i, j int) {
	f.values[i], f.values[j] = f.values[j], f.values[i]
}
func (f *InterpolationBuilder) Set(x, y float64) {
	if y < f.min {
		f.min = y
	}
	if y > f.max {
		f.max = y
	}
	f.values = append(f.values, ival{x, y})
}
func (f *InterpolationBuilder) Init() Function {
	f.values = dedup(f.values)
	sort.Sort(f)
	return &InterpolationI{f.min, f.max, f.values}
}

type ival struct {
	x, y float64
}

type Function interface {
	Domain() (float64, float64)
	Range() (float64, float64)
	Eval(float64) float64
}

type ScaledFunction struct {
	scale float64
	f     Function
}

func NewScaledFunction(scale float64, f Function) *ScaledFunction {
	return &ScaledFunction{scale, f}
}

func (s *ScaledFunction) Domain() (float64, float64) {
	return s.f.Domain()
}
func (s *ScaledFunction) Range() (float64, float64) {
	a, b := s.f.Range()
	return s.scale * a, s.scale * b
}

func (s *ScaledFunction) Eval(x float64) float64 {
	return s.scale * s.f.Eval(x)
}

type InterpolationI struct {
	min    float64
	max    float64
	values []ival
}

func (f *InterpolationI) Domain() (float64, float64) {
	return f.values[0].x, f.values[len(f.values)-1].x
}
func (f *InterpolationI) Range() (float64, float64) {
	return f.min, f.max
}

func (f *InterpolationI) Eval(x float64) float64 {
	switch {
	case x < f.values[0].x:
		return math.NaN()
	case x == f.values[0].x:
		return f.values[0].y
	case x == f.values[len(f.values)-1].x:
		return f.values[len(f.values)-1].y
	case x > f.values[len(f.values)-1].x:
		return math.NaN()
	}
	left := sort.Search(len(f.values), func(i int) bool {
		return x < f.values[i].x
	})
	x0 := f.values[left-1].x
	y0 := f.values[left-1].y
	x1 := f.values[left].x
	y1 := f.values[left].y
	return y0 + (x-x0)*(y1-y0)/(x1-x0)
}

func dedup(list []ival) (out []ival) {
	m := make(map[float64]ival)
	for _, v := range list {
		m[v.x] = v
	}
	for _, v := range m {
		out = append(out, v)
	}
	return
}

func Add(a, b Function, samples int) Function {
	x0, x1 := a.Domain()
	out := NewInterpolationBuilder()
	di := (x1 - x0) / float64(samples-1)
	for i := 0; i < samples; i++ {
		i0 := float64(i)
		x := x0 + i0*di
		out.Set(x, a.Eval(x)+b.Eval(x))
	}
	return out.Init()
}
