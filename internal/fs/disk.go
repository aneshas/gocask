package fs

import (
	"errors"
	"fmt"
	"github.com/aneshas/gocask/pkg/cask"
	gofs "io/fs"
	"os"
	gopath "path"
	"path/filepath"
	"strings"
	"time"
)

// DiskFile represents file on disk
type DiskFile struct {
	*os.File

	size int64
}

// Name returns the base name of the file (without path and/or extension)
func (f *DiskFile) Name() string {
	baseName := filepath.Base(f.File.Name())

	return strings.TrimSuffix(baseName, filepath.Ext(baseName))
}

// Size returns current data file size in kb
func (f *DiskFile) Size() int64 {
	return f.size
}

// Write delegates writing to the underlying file and increments file size
func (f *DiskFile) Write(p []byte) (int, error) {
	f.size += int64(len(p))

	return f.File.Write(p)
}

// NewDisk instantiates new disk based file system
func NewDisk() *Disk {
	return &Disk{}
}

// Disk represents disk based file system
type Disk struct{}

// Open opens a default data file for reading and creates it if it does not exist
func (fs *Disk) Open(path string) (cask.File, error) {
	err := fs.createDir(path)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var dataFile string

	if len(entries) > 0 {
		dataFile = entries[len(entries)-1].Name()
	}

	return fs.openFile(path, dataFile, len(entries))
}

// Rotate creates a new active data file and opens it
func (fs *Disk) Rotate(path string) (cask.File, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	return fs.openFile(path, "", len(entries))
}

func (fs *Disk) openFile(path string, dataFile string, n int) (cask.File, error) {
	if dataFile == "" {
		dataFile = fmt.Sprintf("data_%d_%d.csk", n, time.Now().Unix())
	}

	file, err := os.OpenFile(
		gopath.Join(path, dataFile),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0755,
	)
	if err != nil {
		return nil, fmt.Errorf("could not open db: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &DiskFile{
		file,
		info.Size(),
	}, nil
}

func (fs *Disk) createDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return os.MkdirAll(path, 0755)
	}

	if !info.IsDir() {
		return fmt.Errorf("file exists and it's not a folder")
	}

	return nil
}

func (fs *Disk) Walk(path string, wf func(cask.File) error) error {
	return filepath.Walk(path, func(p string, info gofs.FileInfo, err error) error {
		if info.IsDir() || gopath.Ext(p) != ".csk" {
			return nil
		}

		file, err := os.OpenFile(p, os.O_RDONLY, 0755)
		if err != nil {
			return err
		}

		err = wf(&DiskFile{file, 0})
		if err != nil {
			e := file.Close()
			if e != nil {
				return e
			}

			return err
		}

		return file.Close()
	})
}

func (fs *Disk) ReadFileAt(path string, file string, b []byte, o int64) (int, error) {
	f, err := os.OpenFile(gopath.Join(path, fmt.Sprintf("%s%s", file, ".csk")), os.O_RDONLY, 0755)
	if err != nil {
		return 0, err
	}

	n, err := f.ReadAt(b, o)
	if err != nil {
		return n, err
	}

	return n, f.Close()
}
