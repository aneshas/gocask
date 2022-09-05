package disk

import (
	"errors"
	"fmt"
	"github.com/aneshas/gocask/internal/cask"
	"os"
	ospath "path"
	"path/filepath"
)

// File represents file on disk
type File struct {
	*os.File

	// TODO - Override Write to write in pages?
}

// Name returns the base name of the file (without path and/or extension)
func (fs *File) Name() string {
	return filepath.Base(fs.File.Name())
}

// NewFS instantiates new disk based file system
func NewFS() *FS {
	return &FS{}
}

// FS represents disk based file system
type FS struct {
	// TODO - Maybe keep this as stateless as possible and make it a pure interface to disk
	// eg. for advancing current file just add utility methods that would only be used as serialization point (mutex)
	// plus this can be used for locking the file against multiple processes
}

// Open opens a file for reading and creates it if it does not exist
func (fs *FS) Open(path string) (cask.File, error) {
	currDataFile := "data.csk"

	err := fs.createDir(path)
	if err != nil {
		return nil, err
	}

	dataFile := fmt.Sprintf("%s/%s", path, currDataFile)

	file, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not open db: %w", err)
	}

	return &File{
		file,
	}, nil
}

func (fs *FS) createDir(path string) error {
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

func (fs *FS) Walk(path string, wf func(cask.File) error) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	for _, e := range entries {
		file, err := os.OpenFile(ospath.Join(path, e.Name()), os.O_RDONLY, 0755)
		if err != nil {
			return err
		}

		err = wf(&File{file})
		if err != nil {
			err = file.Close()
			if err != nil {
				return err
			}

			return err
		}

		err = file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (fs *FS) ReadFileAt(path string, file string, b []byte, o int64) (int, error) {
	f, err := os.OpenFile(ospath.Join(path, file, ".csk"), os.O_RDONLY, 0755)
	if err != nil {
		return 0, err
	}

	defer f.Close()

	return f.ReadAt(b, o)
}
