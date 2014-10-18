package goutil

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type WorkSubmitter interface {
	// submit a function to queue
	Submit(f func())
}

type WorkQueue interface {
	WorkSubmitter
	// close queue and wait for all functions to execute
	Wait()
}

func NewWorkQueue(n int) WorkQueue {
	var wg sync.WaitGroup
	ch := make(chan func())
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range ch {
				f()
			}
		}()
	}
	return iWorkQueue{wg: &wg, ch: ch}
}

type iWorkQueue struct {
	wg *sync.WaitGroup
	ch chan func()
}

func (w iWorkQueue) Submit(f func()) {
	w.ch <- f
}

func (w iWorkQueue) Wait() {
	close(w.ch)
	w.wg.Wait()
}

type Tool struct {
}

func (Tool) Name() string {
	return "testq,test the work queue"
}

func (t Tool) Run(args []string) {
	q := NewWorkQueue(100)

	var started, ended int64

	work := func() {
		atomic.AddInt64(&started, 1)
		fmt.Printf("%d\n", started)
		time.Sleep(time.Duration(30+rand.Intn(70)) * time.Millisecond)
		atomic.AddInt64(&ended, 1)
	}

	for i := 0; i < 3000; i++ {
		q.Submit(work)
	}

	q.Wait()

	fmt.Println(started, ended)

}
