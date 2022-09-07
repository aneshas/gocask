package gocask

import (
	"github.com/aneshas/gocask/internal/fs"
	"github.com/aneshas/gocask/pkg/cask"
	"time"
)

// DB represents gocask database
type DB struct {
	*cask.DB
}

// TODO Magic dbPath string	to open in memory db

// Open opens an existing database at dbPath or creates a new one.
// Magic in:mem:db value for dbPath can be used in order to instantiate an in memory file system.
func Open(dbPath string) (*DB, error) {
	var caskFS cask.FS

	caskFS = fs.NewDisk()

	if dbPath == cask.InMemoryDB {
		caskFS = fs.NewInMemory()
	}

	var t goTime

	db, err := cask.NewDB(dbPath, caskFS, t)
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
