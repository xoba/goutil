// reader factory
package rf

import (
	"io"
	"os"
)

func CreateReaderFact(path string) (*FileReaderFact, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &FileReaderFact{Path: path, Length: uint64(fi.Size())}, nil
}

type FileReaderFact struct {
	Path   string
	Length uint64
}

func (rf FileReaderFact) Len() uint64 {
	return rf.Length
}

func (rf FileReaderFact) CreateReader() (io.ReadCloser, error) {
	f, err := os.Open(rf.Path)
	if err != nil {
		return nil, err
	}
	return f, nil
}
