package fs

import (
	"bytes"
	"github.com/aneshas/gocask/pkg/cask"
	"io"
)

type InMemoryFile struct {
	name   string
	reader io.Reader
	f      func([]byte)
}

func (i *InMemoryFile) Read(p []byte) (n int, err error) {
	return i.reader.Read(p)
}

func (i *InMemoryFile) Write(p []byte) (int, error) {
	i.f(p)

	return len(p), nil
}

func (i *InMemoryFile) Close() error {
	return nil
}

func (i *InMemoryFile) Name() string {
	return i.name
}

func (i *InMemoryFile) Size() int64 {
	return 0
}

type InMemory struct {
	b           []byte
	currentFile *InMemoryFile
}

func NewInMemory() *InMemory {
	return &InMemory{}
}

func (i *InMemory) Open(_ string) (cask.File, error) {
	i.currentFile = &InMemoryFile{
		name:   "data",
		reader: bytes.NewReader(i.b),
		f: func(buf []byte) {
			i.b = append(i.b, buf...)
		},
	}

	return i.currentFile, nil
}

func (i *InMemory) Rotate(_ string) (cask.File, error) {
	return i.currentFile, nil
}

func (i *InMemory) Walk(_ string, f func(cask.File) error) error {
	file := &InMemoryFile{
		name:   "data",
		reader: bytes.NewReader(i.b),
	}

	err := f(file)
	if err != nil {
		return err
	}

	return nil
}

func (i *InMemory) ReadFileAt(_ string, _ string, b []byte, offset int64) (int, error) {
	copy(b, i.b[offset:])

	return len(b), nil
}
