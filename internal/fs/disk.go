package fs

import (
	"errors"
	"fmt"
	"github.com/aneshas/gocask/core"
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
func (fs *Disk) Open(path string) (core.File, error) {
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

	return fs.openDataFile(path, dataFile, len(entries))
}

// Rotate creates a new active data file and opens it
func (fs *Disk) Rotate(path string) (core.File, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	return fs.openDataFile(path, "", len(entries))
}

func (fs *Disk) openDataFile(path string, dataFile string, n int) (core.File, error) {
	if dataFile == "" {
		dataFile = fmt.Sprintf("data_%d_%d%s", n, time.Now().Unix(), core.DataFileExt)
	}

	return fs.openFile(gopath.Join(path, dataFile), os.O_RDWR|os.O_CREATE|os.O_APPEND)
}

// OTruncate opens file and truncates it to zero size
// If the file does not exist it will be created
func (fs *Disk) OTruncate(path, file string) (core.File, error) {
	return fs.openFile(gopath.Join(path, fs.toDataFile(file)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
}

func (fs *Disk) openFile(dataFile string, flag int) (core.File, error) {
	file, err := os.OpenFile(dataFile, flag, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not open data file: %w", err)
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

func (fs *Disk) Walk(path string, wf func(core.File) error) error {
	hints := make(map[string]bool)

	return filepath.Walk(path, func(p string, info gofs.FileInfo, err error) error {
		if info.IsDir() ||
			gopath.Ext(p) != core.DataFileExt ||
			strings.Contains(p, core.TmpFileExt) {
			return nil
		}

		if strings.Contains(p, core.HintFileExt) {
			hints[strings.Replace(p, core.HintFileExt, "", 1)] = true
		} else {
			if _, ok := hints[strings.Replace(p, core.DataFileExt, "", 1)]; ok {
				return nil
			}
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
	f, err := os.OpenFile(gopath.Join(path, fs.toDataFile(file)), os.O_RDONLY, 0755)
	if err != nil {
		return 0, err
	}

	n, err := f.ReadAt(b, o)
	if err != nil {
		return n, err
	}

	return n, f.Close()
}

// Move moves src to dst
func (fs *Disk) Move(path string, src string, dst string) error {
	return os.Rename(
		gopath.Join(path, fs.toDataFile(src)),
		gopath.Join(path, fs.toDataFile(dst)),
	)
}

func (fs *Disk) toDataFile(name string) string {
	return fmt.Sprintf("%s%s", name, core.DataFileExt)
}
