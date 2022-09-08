package testutil

import (
	"bytes"
	"github.com/aneshas/gocask/pkg/cask"
	mocks2 "github.com/aneshas/gocask/pkg/cask/testutil/mocks"
	"github.com/stretchr/testify/mock"
	"io"
	"testing"
)

// FS is a mock FS
type FS struct {
	*mocks2.FS

	mockFiles      map[string][]byte
	mockFilesOrder []string

	file     *mocks2.File
	Path     string
	DataFile string
}

// NewFS creates new mock FS
func NewFS() *FS {
	return &FS{
		FS:        &mocks2.FS{},
		Path:      "path/to/db",
		DataFile:  "data",
		mockFiles: make(map[string][]byte),
	}
}

// WithWriteSupport setup
func (fs *FS) WithWriteSupport() *FS {
	var file mocks2.File

	file.On("Name").Return(fs.DataFile)
	file.On("Write", mock.Anything).Return(0, nil)
	file.On("Close").Return(nil)

	fs.On("Open", fs.Path).Return(&file, nil)
	fs.On("Walk", fs.Path, mock.Anything).Return(nil)

	fs.file = &file

	return fs
}

// WithFailWithErrOnWrite setup
func (fs *FS) WithFailWithErrOnWrite(err error) *FS {
	var file mocks2.File

	file.On("Name").Return(fs.DataFile)
	file.On("Write", mock.Anything).Return(0, err)
	file.On("Close").Return(nil)

	fs.On("Open", fs.Path).Return(&file, nil)
	fs.On("Walk", fs.Path, mock.Anything).Return(nil)

	fs.file = &file

	return fs
}

// AddMockDataFileEntry adds entry to a mock data file
func (fs *FS) AddMockDataFileEntry(fName string, entry []byte) {
	found := false

	for _, f := range fs.mockFilesOrder {
		if f == fName {
			found = true
			break
		}
	}

	if !found {
		fs.mockFilesOrder = append(fs.mockFilesOrder, fName)
	}

	fs.mockFiles[fName] = AppendBytes(
		fs.mockFiles[fName],
		entry,
	)
}

// UseMockDataFiles uses mocked in memory files (set by AddMockDataFileEntry)
func (fs *FS) UseMockDataFiles() *FS {
	fs.On("Walk", fs.Path, mock.Anything).
		Run(func(args mock.Arguments) {
			f := args[1].(func(cask.File) error)

			for _, name := range fs.mockFilesOrder {
				_ = f(&echoFile{
					name:   name,
					buffer: bytes.NewReader(fs.mockFiles[name]),
				})
			}
		}).
		Return(nil)

	var file mocks2.File

	file.On("Name").Return(fs.DataFile)
	file.On("Write", mock.Anything).Return(0, nil)
	file.On("Close").Return(nil)

	fs.On("Open", fs.Path).Return(&file, nil)

	fs.On("ReadFileAt", fs.Path, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			name := args[1].(string)
			dest := args[2].([]byte)
			pos := args[3].(int64)

			for i := range dest {
				dest[i] = fs.mockFiles[name][int(pos)+i]
			}
		}).
		Return(0, nil)

	return fs
}

func (fs *FS) WithFailOnReadValueFromFile(err error) *FS {
	fs.On("ReadFileAt", fs.Path, mock.Anything, mock.Anything, mock.Anything).Return(0, err)

	return fs
}

// VerifyEntryWritten verifies that entry was written
func (fs *FS) VerifyEntryWritten(t *testing.T, entry []byte) {
	fs.file.AssertCalled(t, "Write", entry)
}

type echoFile struct {
	name   string
	buffer io.Reader
}

func (e *echoFile) Read(p []byte) (n int, err error) {
	return e.buffer.Read(p)
}

func (e *echoFile) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (e *echoFile) Seek(offset int64, whence int) (int64, error) {
	b := make([]byte, offset)

	// Simulate Seek
	e.buffer.Read(b)

	return 0, nil
}

func (e *echoFile) Close() error {
	return nil
}

func (e *echoFile) Name() string {
	return e.name
}
