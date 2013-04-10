package mr

import (
	"bytes"
	"container/list"
	"errors"
	"sort"
	"sync"
)

func NewLocalFramework(mappers, reducers int) Framework {
	return &_framework{
		mappers:  mappers,
		reducers: reducers,
		input:    make(chan KeyValue, 2*mappers),
		output:   make(chan KeyValue, 2*reducers),
	}
}

type Framework interface {
	Run(mr MapReducer, d Driver) (<-chan KeyValue, error)
}

type MapReducer interface {
	Map(input <-chan KeyValue, collector chan<- KeyValue)
	Reduce(jobs <-chan ReduceJob, collector chan<- KeyValue)
}

type KeyValue struct {
	Key   string
	Value Value
}

type Value map[string]interface{}

type ValueComparator interface {
	Less(i, j Value) bool
}

type Driver interface {
	Drive(c chan<- KeyValue)
}

type ReduceJob struct {
	Key    string
	Values <-chan Value
}

type _framework struct {
	used              bool
	mappers, reducers int
	input, output     chan KeyValue
}

func (f *_framework) Run(mr MapReducer, d Driver) (<-chan KeyValue, error) {
	if f.used {
		return nil, errors.New("framework already used")
	}
	f.used = true
	go f.runMapper(mr)
	go d.Drive(f.input)
	return f.output, nil
}

func (f _framework) runMapper(mr MapReducer) {

	collector := make(chan KeyValue, 2*f.mappers)

	go f.runMapCollectorAndShuffle(collector, mr)

	wg := sync.WaitGroup{}

	for i := 0; i < f.mappers; i++ {
		wg.Add(1)
		per := make(chan KeyValue, 2*f.mappers)
		go func() {
			for v := range per {
				collector <- v
			}
			wg.Done()
		}()
		go mr.Map(f.input, per)
	}

	wg.Wait()

	close(collector)
}

func (f _framework) runMapCollectorAndShuffle(collector chan KeyValue, mr MapReducer) {

	shuf := list.New()
	for kv := range collector {
		shuf.PushBack(kv)
	}

	// if mapreducer is also valuecomparator...
	vc, _ := mr.(ValueComparator)

	slice := createKVSlice(shuf, vc)

	sort.Sort(slice)

	f.runReducer(mr, slice)
}

func (f _framework) runReducer(mr MapReducer, slice keyValueSlice) {

	jobs := make(chan ReduceJob, 2*f.reducers)

	wg := sync.WaitGroup{}

	for i := 0; i < f.reducers; i++ {
		wg.Add(1)
		per := make(chan KeyValue, 2*f.reducers)
		go func() {
			for v := range per {
				f.output <- v
			}
			wg.Done()
		}()
		go mr.Reduce(jobs, per)
	}

	var lastKey string
	first := true
	var job ReduceJob
	var values chan Value

	for _, x := range slice.Slice {
		key := x.Key
		if key != lastKey || first {
			valueCloser(values)
			values = make(chan Value) // synchronous, to preserve ordering
			job = ReduceJob{Key: key, Values: values}
			jobs <- job
			first = false
		}
		values <- x.Value
		lastKey = key
	}

	valueCloser(values)

	close(jobs)

	wg.Wait()

	close(f.output)

}

func valueCloser(v chan Value) {
	if v != nil {
		close(v)
	}
}

type keyValueSlice struct {
	Slice      []KeyValue
	Comparator ValueComparator
}

func (m keyValueSlice) Len() int {
	return len(m.Slice)
}

func (m keyValueSlice) Swap(i, j int) {
	tmp := m.Slice[i]
	m.Slice[i] = m.Slice[j]
	m.Slice[j] = tmp
}

func (m keyValueSlice) Less(i, j int) bool {

	i0 := m.Slice[i]
	j0 := m.Slice[j]

	cmp := bytes.Compare([]byte(i0.Key), []byte(j0.Key))

	if cmp == 0 && m.Comparator != nil {
		return m.Comparator.Less(i0.Value, j0.Value)
	} else {
		return cmp < 0
	}
}

func createKVSlice(x *list.List, vc ValueComparator) keyValueSlice {

	out := make([]KeyValue, x.Len())
	i := 0
	for e := x.Front(); e != nil; e = e.Next() {
		out[i] = e.Value.(KeyValue)
		i++
	}
	return keyValueSlice{Slice: out, Comparator: vc}
}
