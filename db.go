package gocask

import (
	"github.com/aneshas/gocask/internal/cask"
	"github.com/aneshas/gocask/internal/fs"
	"time"
)

// DB represents gocask database
type DB struct {
	*cask.DB
}

// TODO Magic dbPath string	to open in memory db

// Open opens an existing database at dbPath or creates a new one.
func Open(dbPath string) (*DB, error) {
	db, err := cask.NewDB(dbPath, fs.NewDisk(), goTime{})
	if err != nil {
		return nil, err
	}

	return &DB{
		db,
	}, nil
}

type goTime struct{}

func (t goTime) NowUnix() uint32 {
	return uint32(time.Now().UTC().Unix())
}
