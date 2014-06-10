package java

import (
	"bytes"
	"encoding/binary"
	"io"
	"os/exec"
	"time"
)

type Jchan struct {
	cmd *exec.Cmd
	w   io.Writer
	r   io.Reader
}

func NewChan(cmd *exec.Cmd) (*Jchan, error) {
	w, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	r, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	return &Jchan{
		cmd: cmd,
		w:   w,
		r:   r,
	}, nil
}

func (j *Jchan) WriteByte(i byte) error {
	if _, err := j.w.Write([]byte{i}); err != nil {
		return err
	}
	return nil
}

func (j *Jchan) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	n, err := j.r.Read(buf)
	if err != nil || n != 1 {
		return 0, err
	}
	return buf[0], nil
}

func (j *Jchan) WriteInt64(i int64) error {
	if err := binary.Write(j.w, binary.BigEndian, i); err != nil {
		return err
	}
	return nil
}
func (j *Jchan) WriteTime(t time.Time) error {
	i := t.UnixNano() / 1000000
	if err := binary.Write(j.w, binary.BigEndian, i); err != nil {
		return err
	}
	return nil
}

func (j *Jchan) WriteInt32(i int32) error {
	if err := binary.Write(j.w, binary.BigEndian, i); err != nil {
		return err
	}
	return nil
}

func (j *Jchan) ReadInt32() (int32, error) {
	var i int32
	if err := binary.Read(j.r, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *Jchan) ReadInt64() (int64, error) {
	var i int64
	if err := binary.Read(j.r, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *Jchan) ReadInt16() (int16, error) {
	var i int16
	if err := binary.Read(j.r, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *Jchan) WriteFloat64(i float64) error {
	if err := binary.Write(j.w, binary.BigEndian, i); err != nil {
		return err
	}
	return nil
}

func (j *Jchan) ReadFloat32() (float32, error) {
	var i float32
	if err := binary.Read(j.r, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *Jchan) ReadFloat64() (float64, error) {
	var i float64
	if err := binary.Read(j.r, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *Jchan) WriteString(i string) error {
	var x int32
	x = int32(len(i))
	if err := binary.Write(j.w, binary.BigEndian, x); err != nil {
		return err
	}
	if _, err := j.w.Write([]byte(i)); err != nil {
		return err
	}
	return nil
}

func (j *Jchan) ReadString() (string, error) {
	n, err := j.ReadInt32()
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	_, err = io.CopyN(buf, j.r, int64(n))
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (j *Jchan) WriteBool(i bool) error {
	var x byte
	if i {
		x = 1
	} else {
		x = 0
	}
	if err := binary.Write(j.w, binary.BigEndian, x); err != nil {
		return err
	}
	return nil
}
