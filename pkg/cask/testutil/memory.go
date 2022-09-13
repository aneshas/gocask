package testutil

import (
	"bytes"
	"fmt"
	"github.com/aneshas/gocask/pkg/cask"
)

type InMemoryFile struct {
	file cask.File
	fs   *InMemory
}

func (i *InMemoryFile) Read(p []byte) (n int, err error) {
	return i.file.Read(p)
}

func (i *InMemoryFile) Write(p []byte) (int, error) {
	if i.fs.pwKey != nil && bytes.Contains(p, i.fs.pwKey) {

		l := len(p) - 1

		i.file.Write(p[:l])

		return l, fmt.Errorf("entry written partially")
	}

	return i.file.Write(p)
}

func (i *InMemoryFile) Close() error {
	return i.file.Close()
}

func (i *InMemoryFile) Name() string {
	return i.file.Name()
}

func (i *InMemoryFile) Size() int64 {
	return 0
}

type InMemory struct {
	fs    cask.FS
	pwKey []byte
}

func NewInMemory(fs cask.FS) *InMemory {
	return &InMemory{
		fs: fs,
	}
}

func (i *InMemory) Open(path string) (cask.File, error) {
	f, err := i.fs.Open(path)
	if err != nil {
		return nil, err
	}

	return &InMemoryFile{
		file: f,
		fs:   i,
	}, nil
}

func (i *InMemory) Rotate(path string) (cask.File, error) {
	return i.Rotate(path)
}

func (i *InMemory) Walk(path string, f func(cask.File) error) error {
	return i.fs.Walk(path, f)
}

func (i *InMemory) ReadFileAt(path string, file string, b []byte, offset int64) (int, error) {
	return i.fs.ReadFileAt(path, file, b, offset)
}

func (i *InMemory) WithPartialWriteFor(key []byte) *InMemory {
	i.pwKey = key

	return i
}
