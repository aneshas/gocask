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
)

// DiskFile represents file on disk
type DiskFile struct {
	*os.File
}

// Name returns the base name of the file (without path and/or extension)
func (fs *DiskFile) Name() string {
	baseName := filepath.Base(fs.File.Name())

	return strings.TrimSuffix(baseName, filepath.Ext(baseName))
}

// NewDisk instantiates new disk based file system
func NewDisk() *Disk {
	return &Disk{}
}

// TODO - write in pages ? (this would be handled in DiskFile abstraction)

// Disk represents disk based file system
type Disk struct{}

// Open opens a default data file for reading and creates it if it does not exist
func (fs *Disk) Open(path string) (cask.File, error) {
	dataFile := "data.csk"

	err := fs.createDir(path)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(
		fmt.Sprintf("%s/%s", path, dataFile),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0755,
	)
	if err != nil {
		return nil, fmt.Errorf("could not open db: %w", err)
	}

	return &DiskFile{
		file,
	}, nil
}

func (fs *Disk) createDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return os.Mkdir(path, 0755)
	}

	if !info.IsDir() {
		return fmt.Errorf("file exists and it's not a folder")
	}

	return nil
}

func (fs *Disk) Walk(path string, wf func(cask.File) error) error {
	return filepath.Walk(path, func(p string, info gofs.FileInfo, err error) error {
		// TODO err ?

		if info.IsDir() || gopath.Ext(p) != ".csk" {
			return nil
		}

		file, err := os.OpenFile(p, os.O_RDONLY, 0755)
		if err != nil {
			return err
		}

		err = wf(&DiskFile{file})
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
