/*
logging for the purposes of analysis later on.

*/
package log

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/xoba/goutil"
	"github.com/xoba/goutil/aws"
	"github.com/xoba/goutil/aws/s3"
)

type Logger interface {

	// async logging
	Add(interface{})

	// returns only when log is committed to permanent storage
	AddSync(interface{}) error

	Flush()
}

type test struct {
	bucket string
	a      aws.Auth
}

func NewTest(a aws.Auth, bucket string) *test {
	return &test{bucket, a}
}

func (*test) Name() string {
	return "log.test,test logging code"
}

func (t *test) Run(args []string) {
	ss3 := s3.GetDefault(t.a)
	logger := NewJSONLogger("myrun", ss3, t.bucket, "/tmp")
	i := 0
	for {
		i++
		fmt.Printf("running #%d\n", i)
		logger.Add(fmt.Sprintf("this is a test #%d", i))
		time.Sleep(100 * time.Millisecond)
	}
}

type logger struct {
	run      string
	ss3      s3.Interface
	bucket   string
	dir      string
	messages chan Message2
}

type Message2 struct {
	Message
	Type  string
	Reply chan error
}

type Message struct {
	Time    time.Time
	Id      string
	Payload interface{}
}

func NewJSONLogger(runID string, ss3 s3.Interface, bucket string, dir string) Logger {
	log := logger{
		bucket:   bucket,
		ss3:      ss3,
		run:      runID,
		dir:      dir,
		messages: make(chan Message2, 100),
	}
	go log.poll()
	return &log
}

/*

 periodically writes out to a file, then saves file to s3

*/
func (log *logger) poll() {

	var path string
	var last time.Time
	var e *json.Encoder
	var items []Message
	var count int
	var replies []chan error

	reset := func() func() {
		var err error
		var f *os.File
		return func() {
			for _, c := range replies {
				go func(c chan error) {
					c <- nil
				}(c)
			}
			replies = make([]chan error, 0)
			if len(path) > 0 {
				f.Close()
				err = os.Remove(path)
				if err != nil {
					fmt.Fprintf(os.Stderr, "oops... can't remove %s: %v\n", path, err)
				}
			}
			path = log.dir + "/" + uuid.New() + ".json"
			f, err = os.Create(path)
			check(err)
			last = time.Now()
			e = json.NewEncoder(f)
			items = make([]Message, 0)
			count = 0
		}
	}()

	reset()

	ticker := time.NewTicker(100 * time.Millisecond)

	for {

		select {

		case <-ticker.C:

			if len(items) > 0 {
				rec := LogRecord{
					Run:      log.run,
					Time:     time.Now().UTC(),
					Id:       uuid.New(),
					Messages: items,
				}
				err := e.Encode(rec)
				if err != nil {
					fmt.Fprintf(os.Stderr, "oops... error writing log: %v\n", err)
				}
				count += len(items)
				items = make([]Message, 0)
			}

			if count > 0 && time.Now().Sub(last) > 10*time.Second {
				log.saveToS3(path)
				reset()
			}

		case m := <-log.messages:

			switch m.Type {

			case "flush":

				if count > 0 {
					log.saveToS3(path)
					reset()
				}

			default:
				items = append(items, m.Message)
				if m.Reply != nil {
					replies = append(replies, m.Reply)
				}
			}
		}

	}
}

func (log *logger) Flush() {
	log.messages <- Message2{
		Type: "flush",
	}
}

func (log *logger) Add(x interface{}) {
	log.messages <- Message2{
		Message: Message{
			Time:    time.Now().UTC(),
			Id:      uuid.New(),
			Payload: x,
		},
	}
	return
}

func (log *logger) AddSync(x interface{}) error {
	reply := make(chan error)
	log.messages <- Message2{
		Message: Message{
			Time:    time.Now().UTC(),
			Id:      uuid.New(),
			Payload: x,
		},
		Reply: reply,
	}
	return <-reply
}

type LogRecord struct {
	Run      string
	Id       string
	Time     time.Time
	Messages []Message
}

func (log *logger) saveToS3(path string) error {
	rf, err := goutil.NewFileReaderFact(path)
	if err != nil {
		return err
	}
	o := s3.Object{
		Bucket: log.bucket,
		Key:    fmt.Sprintf("%s_%s_%s.json", uuid.New(), log.run, time.Now().UTC().Format("20060102T150405Z")),
	}
	return log.ss3.Put(s3.PutRequest{
		BasePut: s3.BasePut{
			Object:      o,
			ContentType: "application/json",
		},
		ReaderFact: rf,
	})
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
