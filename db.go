package gocask

import (
	"github.com/aneshas/gocask/internal/fs"
	"github.com/aneshas/gocask/pkg/cask"
	"os"
	"path"
	"time"
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

// DB represents gocask
// A Log-Structured Hash Table for Fast Key/Value Data
// Based on https://riak.com/assets/bitcask-intro.pdf
type DB struct {
	*cask.DB
}

// Open opens an existing database at dbPath or creates a new one
// The database location can be configured with config options and the default is ~/gcdata
// Magic in:mem:db value for dbPath can be used in order to instantiate an in memory file system
// which can be used for testing purposes
func Open(dbPath string, opts ...Option) (*DB, error) {
	var caskFS cask.FS

	caskFS = fs.NewDisk()

	if dbPath == cask.InMemoryDB {
		caskFS = fs.NewInMemory()
	}

	var t goTime

	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	cfg := cask.Config{
		MaxDataFileSize: 10 * GB,
		DataDir:         path.Join(home, "gcdata"),
	}

	for _, opt := range opts {
		cfg = opt(cfg)
	}

	db, err := cask.NewDB(dbPath, caskFS, t, cfg)
	if err != nil {
		return nil, err
	}

	return &DB{
		db,
	}, nil
}

// Option represents gocask configuration option
type Option func(config cask.Config) cask.Config

// WithMaximumDataFileSize configures maximum data file size after which
// data files will be rotated
func WithMaximumDataFileSize(bytes int64) Option {
	return func(config cask.Config) cask.Config {
		config.MaxDataFileSize = bytes

		return config
	}
}

// WithDataDir configures the location of the data dir where your databases will reside
func WithDataDir(path string) Option {
	return func(config cask.Config) cask.Config {
		config.DataDir = path

		return config
	}
}

// TODO
// Max file size config (maybe not in bytes?)
// Default data folder config

type goTime struct{}

// NowUnix returns current unix timestamp
func (t goTime) NowUnix() uint32 {
	return uint32(time.Now().UTC().Unix())
}
