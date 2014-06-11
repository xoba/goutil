package jdbc

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/xoba/goutil/java"
)

type Driver struct {
}

func (j Driver) Open(name string) (driver.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}
	v := u.Query()
	classPath := func() string {
		var out []string
		out = append(out, ".")
		for _, j := range v["jar"] {
			out = append(out, j)
		}
		return strings.Join(out, ":")
	}()
	connStr := v.Get("str")
	driver := v.Get("driver")
	user := v.Get("user")
	pass := v.Get("pass")
	if len(classPath) == 0 {
		panic("needs classpath")
	}
	if len(classPath) == 0 {
		panic("needs classpath")
	}
	if len(connStr) == 0 {
		panic("needs jdbc connection string")
	}
	if len(driver) == 0 {
		panic("needs driver classname")
	}
	if len(user) == 0 {
		panic("needs user")
	}
	if len(pass) == 0 {
		panic("needs password")
	}
	checkJava()
	cmd := exec.Command("java", "-cp", classPath, "GoJdbc", driver, connStr, user, pass)
	cmd.Stderr = os.Stderr
	ch, err := java.NewChan(cmd)
	if err != nil {
		return nil, err
	}
	return &conn{ch}, nil
}

func checkJava() {
	_, err := os.Stat("GoJdbc.class")
	if err != nil {
		f, err := os.Create("GoJdbc.java")
		check(err)
		f.Write([]byte(javasrc))
		f.Close()
		check(exec.Command("javac", "GoJdbc.java").Run())
		os.Remove("GoJdbc.java")
	}
}

type conn struct {
	ch *java.Jchan
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	err := c.ch.WriteByte(2)
	if err != nil {
		return nil, err
	}
	id := uuid.New()
	err = c.ch.WriteString(id)
	if err != nil {
		return nil, err
	}
	err = c.ch.WriteString(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, id: id, query: query}, nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func (c *conn) Close() error {
	err := c.ch.WriteByte(1)
	if err != nil {
		return err
	}
	_, err = c.ch.ReadByte()
	check(err)
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return &tx{c.ch}, c.ch.WriteByte(11)
}

type tx struct {
	ch *java.Jchan
}

func (t *tx) Commit() error {
	return t.ch.WriteByte(12)
}
func (t *tx) Rollback() error {
	return t.ch.WriteByte(13)
}

type stmt struct {
	*conn
	id    string
	query string
}

func (s *stmt) Close() error {
	s.conn.ch.WriteByte(9)
	s.conn.ch.WriteString(s.id)
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	for i, x := range args {
		switch x := x.(type) {
		case int64:
			s.conn.ch.WriteByte(3)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteInt64(x)
		case string:
			s.conn.ch.WriteByte(4)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteString(x)
		case float64:
			s.conn.ch.WriteByte(8)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteFloat64(x)
		case time.Time:
			s.conn.ch.WriteByte(14)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteInt64(x.UnixNano() / 1000000)

		default:
			fmt.Printf("unhandled: %T %v\n", x, x)
		}
	}
	s.conn.ch.WriteByte(5)
	s.conn.ch.WriteString(s.id)
	b, err := s.conn.ch.ReadByte()
	check(err)
	id2 := uuid.New()
	switch b {
	case 0:
		c, err := s.conn.ch.ReadInt32()
		check(err)
		if c < 0 {
			c = 0
		}
		return result{s.conn, id2, int64(c)}, nil
	case 1:
		panic("didn't expect resultset")
	case 2:
		e, err := s.conn.ch.ReadString()
		if err != nil {
			return nil, err
		}
		return nil, errors.New(e)
	default:
		panic("oops")
	}
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	for i, x := range args {
		switch x := x.(type) {
		case int64:
			s.conn.ch.WriteByte(3)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteInt64(x)
		case string:
			s.conn.ch.WriteByte(4)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteString(x)
		case float64:
			s.conn.ch.WriteByte(8)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteFloat64(x)
		default:
			fmt.Printf("unhandled: %T %v\n", x, x)
		}
	}
	s.conn.ch.WriteByte(5)
	s.conn.ch.WriteString(s.id)
	b, err := s.conn.ch.ReadByte()
	check(err)
	switch b {
	case 0:
		_, err := s.conn.ch.ReadInt32()
		check(err)
		return &norows{}, nil
	case 1:
		var names, classes []string
		id2 := uuid.New()
		s.conn.ch.WriteString(id2)
		n, err := s.conn.ch.ReadInt32()
		check(err)
		for i := 0; i < int(n); i++ {
			name, err := s.conn.ch.ReadString()
			check(err)
			class, err := s.conn.ch.ReadString()
			check(err)
			names = append(names, name)
			classes = append(classes, class)
		}
		return &rows{s.conn, id2, names, classes}, nil
	case 2:
		e, err := s.conn.ch.ReadString()
		if err != nil {
			return nil, err
		}
		return nil, errors.New(e)
	default:
		panic("oops")
	}
}

type rows struct {
	*conn
	id      string
	names   []string
	classes []string
}

func (r *rows) Columns() []string {
	return r.names
}
func (r *rows) Close() error {
	r.conn.ch.WriteByte(10)
	r.conn.ch.WriteString(r.id)
	return nil
}
func (r *rows) Next(dest []driver.Value) error {

	r.conn.ch.WriteByte(6)
	r.conn.ch.WriteString(r.id)
	b, err := r.conn.ch.ReadByte()
	check(err)
	if b == 0 {
		return io.EOF
	}
	for i, _ := range r.names {
		r.conn.ch.WriteByte(7)
		r.conn.ch.WriteString(r.id)
		r.conn.ch.WriteInt32(int32(i + 1))

		dest[i] = nil

		switch r.classes[i] {
		case "java.lang.Integer":
			r.conn.ch.WriteByte(1)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt32()
				check(err)
				dest[i] = v
			}
		case "java.math.BigDecimal":
			r.conn.ch.WriteByte(10)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadString()
				check(err)
				r := big.NewRat(0, 1)
				_, ok := r.SetString(v)
				if !ok {
					panic("oops: " + v)
				}
				f, _ := r.Float64()
				dest[i] = f
			}
		case "java.lang.Long":
			r.conn.ch.WriteByte(6)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt64()
				check(err)
				dest[i] = v
			}
		case "java.lang.Short":
			r.conn.ch.WriteByte(7)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt16()
				check(err)
				dest[i] = v
			}
		case "java.lang.Byte":
			r.conn.ch.WriteByte(8)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadByte()
				check(err)
				dest[i] = v
			}
		case "java.lang.Boolean":
			r.conn.ch.WriteByte(9)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadByte()
				check(err)
				dest[i] = (v == 1)
			}
		case "java.sql.Date":
			r.conn.ch.WriteByte(5)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt64()
				check(err)
				t := time.Unix(0, v*1000000)
				dest[i] = t
			}
		case "java.lang.String":
			r.conn.ch.WriteByte(2)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadString()
				check(err)
				dest[i] = []byte(v)
			}
		case "java.lang.Double":
			r.conn.ch.WriteByte(3)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadFloat64()
				check(err)
				dest[i] = v
			}
		case "java.lang.Float":
			r.conn.ch.WriteByte(4)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadFloat32()
				check(err)
				dest[i] = v
			}
		default:
			panic(r.classes[i])
		}
	}
	return nil
}

type result struct {
	*conn
	id2 string
	c   int64
}

func (r result) LastInsertId() (int64, error) {
	return 0, nil
}
func (r result) RowsAffected() (int64, error) {
	return r.c, nil
}

type norows struct {
	c int
}

func (r *norows) Columns() []string {
	return nil
}
func (r *norows) Close() error {
	return nil
}
func (r *norows) Next(dest []driver.Value) error {
	return fmt.Errorf("no rows")
}
