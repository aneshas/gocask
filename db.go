package gocask

import (
	"github.com/aneshas/gocask/internal/fs"
	"github.com/aneshas/gocask/pkg/cask"
	"time"
)

// DB represents gocask
// A Log-Structured Hash Table for Fast Key/Value Data
// Based on https://riak.com/assets/bitcask-intro.pdf
type DB struct {
	*cask.DB
}

// Open opens an existing database at dbPath or creates a new one
// Magic in:mem:db value for dbPath can be used in order to instantiate an in memory file system
// which can be used for testing purposes
func Open(dbPath string) (*DB, error) {
	var caskFS cask.FS

	caskFS = fs.NewDisk()

	if dbPath == cask.InMemoryDB {
		caskFS = fs.NewInMemory()
	}

	var t goTime

	db, err := cask.NewDB(dbPath, caskFS, t, cask.DefaultConfig)
	if err != nil {
		return nil, err
	}

	return &DB{
		db,
	}, nil
}

// TODO
// Max file size config (maybe not in bytes?)
// Default data folder config

type goTime struct{}

// NowUnix returns current unix timestamp
func (t goTime) NowUnix() uint32 {
	return uint32(time.Now().UTC().Unix())
}
