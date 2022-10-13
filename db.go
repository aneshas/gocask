package gocask

import (
	"github.com/aneshas/gocask/core"
	"github.com/aneshas/gocask/internal/fs"
	"os"
	"path"
)

const (
	// KB represents base2 kilobyte
	KB int64 = 1024

	// MB represents base2 megabyte
	MB = KB * 1024

	// GB represents base2 gigabyte
	GB = MB * 1024

	// TB represents base2 terabyte
	TB = GB * 1024
)

// Open opens an existing database at dbPath or creates a new one
// The database location can be configured with config options and the default is ~/gcdata
// Magic in:mem:db value for dbPath can be used in order to instantiate an in memory file system
// which can be used for testing purposes.
func Open(dbPath string, opts ...Option) (*core.DB, error) {
	var caskFS core.FS

	var t core.GoTime

	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	cfg := core.Config{
		MaxDataFileSize: 10 * GB,
		DataDir:         path.Join(home, "gcdata"),
	}

	for _, opt := range opts {
		cfg = opt(cfg)
	}

	caskFS = fs.NewDisk(core.GoTime{})

	if dbPath == core.InMemoryDB {
		caskFS = fs.NewInMemory()
	}

	// Schedule compaction on this level and call db.Merge periodically

	db, err := core.NewDB(dbPath, caskFS, t, cfg)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Option represents gocask configuration option
type Option func(config core.Config) core.Config

// WithMaxDataFileSize configures maximum data file size after which
// data files will be rotated
func WithMaxDataFileSize(bytes int64) Option {
	return func(config core.Config) core.Config {
		config.MaxDataFileSize = bytes

		return config
	}
}

// WithDataDir configures the location of the data dir where your databases will reside
func WithDataDir(path string) Option {
	return func(config core.Config) core.Config {
		config.DataDir = path

		return config
	}
}
