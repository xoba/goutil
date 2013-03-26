package mr

import (
	"io/ioutil"
	"sync"
)

// runs one driver at a time
type ChainedDriver struct {
	Drivers []Driver
}

func (x ChainedDriver) Drive(output chan<- KeyValue) {
	for i := 0; i < len(x.Drivers); i++ {
		wg := new(sync.WaitGroup)
		wg.Add(1)
		c := make(chan KeyValue)
		go func() {
			for kv := range c {
				output <- kv
			}
			wg.Done()
		}()
		x.Drivers[i].Drive(c)
		wg.Wait()
	}
	close(output)
}

// runs all drivers in parallel
type MultiDriver struct {
	Drivers []Driver
}

func (x MultiDriver) Drive(output chan<- KeyValue) {

	wg := new(sync.WaitGroup)

	for i := 0; i < len(x.Drivers); i++ {
		wg.Add(1)
		c := make(chan KeyValue)
		go func() {
			for kv := range c {
				output <- kv
			}
			wg.Done()
		}()
		go x.Drivers[i].Drive(c)
	}

	wg.Wait()

	close(output)
}

// drives one channel from another
type ChannelDriver struct {
	Chan <-chan KeyValue
}

func (x ChannelDriver) Drive(output chan<- KeyValue) {
	for kv := range x.Chan {
		output <- kv
	}
	close(output)
}

// drives from files in a directory
type DirDriver struct {
	Dir string
}

func (x DirDriver) Drive(output chan<- KeyValue) {
	d, err := ioutil.ReadDir(x.Dir)
	if err != nil {
		panic(err)
	}
	for _, fi := range d {
		buf, err := ioutil.ReadFile(x.Dir + "/" + fi.Name())
		if err != nil {
			panic(err)
		}
		output <- createKV(fi.Name(), buf)
	}
	close(output)
}

func createKV(name string, buf []byte) KeyValue {
	v := make(map[string]interface{})
	v["content"] = buf
	return KeyValue{Key: name, Value: v}
}
