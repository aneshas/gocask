package testutil

import (
	"github.com/aneshas/gocask/internal/cask/testutil/mocks"
	"github.com/stretchr/testify/mock"
	"testing"
)

type FS struct {
	*mocks.FS

	file     *mocks.File
	Path     string
	DataFile string
}

func NewFS() *FS {
	return &FS{
		FS:       &mocks.FS{},
		Path:     "path/to/db",
		DataFile: "data",
	}
}

func (fs *FS) WithWriteSupport() *FS {
	var file mocks.File

	file.On("Name").Return(fs.DataFile)
	file.On("Write", mock.Anything).Return(0, nil)
	file.On("Close").Return(nil)

	fs.On("Open", fs.Path).Return(&file, nil)

	fs.file = &file

	return fs
}

func (fs *FS) VerifyEntryWritten(t *testing.T, entry []byte) {
	fs.file.AssertCalled(t, "Write", entry)
}
