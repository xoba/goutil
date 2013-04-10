/*

 logging for the purposes of analysis later on (i.e., the basis of big data)

*/
package logging

import (
	"code.google.com/p/go-uuid/uuid"
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/xoba/goutil/aws/s3"
	golog "log"
	"reflect"
	"time"
)

const (
	CAPACITY       = 1000
	PERIOD         = 60 * time.Second
	ISO8601_FORMAT = "20060102T150405Z"
)

type Log interface {
	Add(interface{})
	Flush()
}

type ControlMessage struct {
	Name   string
	Notify chan error
}

type _Log struct {
	list     *list.List
	control  chan ControlMessage
	newItems chan Message
	run      string
	ss3      s3.Interface
	bucket   string
}

func NewLogger(runID string, ss3 s3.Interface, bucket string) Log {
	log := _Log{bucket: bucket, ss3: ss3, run: runID, list: list.New(), control: make(chan ControlMessage, CAPACITY), newItems: make(chan Message, CAPACITY)}
	log.poll()
	return log
}

func (log _Log) Add(x interface{}) {
	t := reflect.TypeOf(x)
	msg := Message{Time: time.Now().UTC(), Pkg: t.PkgPath(), Type: t.Name(), Value: x}
	log.newItems <- msg
}

func (log _Log) Flush() {
	golog.Println("flushing log...")
	cm := ControlMessage{Name: "poll", Notify: make(chan error)}
	log.control <- cm
	res := <-cm.Notify
	close(cm.Notify)
	if res != nil {
		golog.Printf("flushed log error: %s", res)
	}
}

type LogRecord struct {
	Run     string
	Time    time.Time
	Payload []Message
}

type Message struct {
	Time  time.Time
	Pkg   string
	Type  string
	Value interface{}
}

func formatISOTime(t time.Time) string {
	return t.UTC().Format(ISO8601_FORMAT)
}

func saveJSON(ss3 s3.Interface, bucket string, o interface{}, key string) error {

	golog.Printf("saving json to http://%s.s3.amazonaws.com/%s", bucket, key)

	payload, err := json.MarshalIndent(o, "", "  ")

	if err != nil {
		return err
	}

	req := s3.PutObjectRequest{Object: s3.Object{Bucket: bucket, Key: key}, ContentType: "application/json", Data: payload}

	return ss3.PutObject(req)

}

func copyMessages(x *list.List) []Message {
	payload := make([]Message, x.Len())
	i := 0
	for e := x.Front(); e != nil; e = e.Next() {
		payload[i] = e.Value.(Message)
		i++
	}
	return payload
}

func coreSave(ss3 s3.Interface, bucket string, log _Log, payload []Message, notify chan error) {

	now := time.Now().UTC()
	rec := LogRecord{Run: log.run, Time: now, Payload: payload}

	err := saveJSON(ss3, bucket, rec, fmt.Sprintf("%s_%s.json", formatISOTime(now), uuid.New()))

	if notify != nil {
		notify <- err
	}
}

func (log _Log) save(c ControlMessage) {

	if log.list.Len() > 0 {

		payload := copyMessages(log.list)

		log.list.Init()

		go coreSave(log.ss3, log.bucket, log, payload, c.Notify)

	}
}

func (log _Log) checkSave() {
	if log.list.Len() > CAPACITY/10 {
		log.control <- ControlMessage{Name: "save"}
	}
}

func (log _Log) poll() {

	golog.Printf("logger starting, polling every %s", PERIOD)

	ticker := time.Tick(PERIOD)

	go func() {
		for _ = range ticker {
			log.control <- ControlMessage{Name: "poll"}
		}
	}()

	go func() {

		for {

			select {

			case c := <-log.control:

				if c.Name == "save" {
					log.save(c)
				} else if c.Name == "poll" {
					log.save(c)
				}

			case m := <-log.newItems:
				log.list.PushBack(m)

			}

			log.checkSave()
		}

	}()
}
