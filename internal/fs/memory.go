package fs

import (
	"bytes"
	"github.com/aneshas/gocask/internal/cask"
)

type InMemoryFile struct {
	name   string
	buffer *bytes.Buffer
}

func (i *InMemoryFile) Read(p []byte) (n int, err error) {
	return i.buffer.Read(p)
}

func (i *InMemoryFile) Write(p []byte) (int, error) {
	return i.buffer.Write(p)
}

func (i *InMemoryFile) Seek(offset int64, _ int) (int64, error) {
	b := make([]byte, offset)

	n, err := i.buffer.Read(b)

	return int64(n) + offset, err
}

func (i *InMemoryFile) Close() error {
	return nil
}

func (i *InMemoryFile) Name() string {
	return i.name
}

type InMemory struct {
	currentFile *InMemoryFile
}

func NewInMemory() *InMemory {
	return &InMemory{
		currentFile: &InMemoryFile{
			name:   "data",
			buffer: bytes.NewBuffer([]byte{}),
		},
	}
}

func (i InMemory) Open(path string) (cask.File, error) {
	return i.currentFile, nil
}

func (i InMemory) Walk(path string, f func(cask.File) error) error {
	return f(i.currentFile)
}

func (i InMemory) ReadFileAt(path string, file string, b []byte, offset int64) (int, error) {
	cp := i.currentFile.buffer.Bytes()

	for i := range b {
		b[i] = cp[offset+int64(i)]
	}

	return len(cp), nil
}
