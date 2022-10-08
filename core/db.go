package core

import (
	"bufio"
	"errors"
	"github.com/aneshas/gocask/internal/crc"
	"io"
	"path"
	"strings"
	"sync"
)

var (
	// ErrKeyNotFound signifies that the requested key was not found in keydir
	// This effectively means that the key does not exist in the database itself
	ErrKeyNotFound = errors.New("gocask: key not found")

	// ErrPartialWrite signifies that the underlying disk write was not complete, which means
	// that write was successful but the entry is corrupted and the operation should be retried
	ErrPartialWrite = errors.New("gocask: key/value pair not fully written")

	// ErrCRCFailed is thrown upon reading a corrupted value
	ErrCRCFailed = errors.New("gocask: crc check failed for db entry (value is corrupted)")

	// ErrInvalidKey is thrown when attempting Get, Put or Delete with an invalid key
	ErrInvalidKey = errors.New("gocask: key should not be empty or nil")

	// ErrInvalidValue is thrown when attempting to store a nil value
	ErrInvalidValue = errors.New("gocask: value should not be nil")
)

const (
	// InMemoryDB represents a magic value which can be used instead of db path
	// in order to instantiate in memory file system instead of disk
	InMemoryDB = "in:mem:db"
)

// FS represents a file system interface
type FS interface {
	// Open should open the active data file for the given db path
	Open(string) (File, error)

	// OTruncate should open an existing file or create a new one
	// The file should also be truncated to zero size
	OTruncate(string, string) (File, error)

	// Move should move src file to dst replacing it if it exists
	Move(path string, src string, dst string) error

	// Rotate should generate and open new data file for the given db path
	Rotate(string) (File, error)

	// Walk should walk through all data files for the given db path
	Walk(string, func(File) error) error

	// ReadFileAt should read a chunk of named path data file at the given offset
	ReadFileAt(string, string, []byte, int64) (int, error)
}

// File represents a single fs data file
type File interface {
	io.ReadWriteCloser

	Name() string
	Size() int64
}

// Time represents time provider
type Time interface {
	NowUnix() uint32
}

// DB represents a bitcask database
// A Log-Structured Hash Table for Fast Key/Value Data
// Based on https://riak.com/assets/bitcask-intro.pdf
type DB struct {
	cfg  Config
	time Time
	fs   FS
	file File
	path string
	kd   *keyDir
	m    sync.RWMutex
}

// DefaultConfig represents default gocask config
var DefaultConfig = Config{
	MaxDataFileSize: 1024 * 1024 * 1024 * 2,
	DataDir:         "./",
}

// Config represents gocask config
type Config struct {
	MaxDataFileSize int64
	DataDir         string
}

// NewDB instantiates new db with provided FS as storage mechanism
func NewDB(dbpath string, fs FS, time Time, cfg Config) (*DB, error) {
	dbpath = path.Join(cfg.DataDir, dbpath)

	f, err := fs.Open(dbpath)
	if err != nil {
		return nil, err
	}

	caskDB := DB{
		cfg:  cfg,
		time: time,
		fs:   fs,
		file: f,
		path: dbpath,
		kd:   newKeyDir(),
	}

	return &caskDB, caskDB.init()
}

func (db *DB) init() error {
	return db.fs.Walk(db.path, func(file File) error {
		var err error

		if strings.Contains(file.Name(), ".a") {
			err = mapFile(file, db.indexHint)
		} else {
			err = mapFile(file, db.indexEntry)
		}

		if err != nil {
			return err
		}

		if !db.isActive(file) {
			db.kd.resetOffset()
		}

		return nil
	})
}

func (db *DB) indexHint(r *bufio.Reader, file string) error {
	hint, err := parseHintEntry(r)
	if err != nil {
		return err
	}

	db.kd.setFromHint(hint, strings.TrimSuffix(file, hintFilePartial))

	return err
}

func (db *DB) indexEntry(r *bufio.Reader, file string) error {
	ke, err := parseKEntry(r)
	if err != nil {
		return err
	}

	if ke.isTombstone() {
		db.kd.unset(ke.key)

		return nil
	}

	_, err = r.Discard(int(ke.ValueSize))
	if err != nil {
		return err
	}

	db.kd.set(ke.key, ke.header, file)

	return nil
}

// Close performs db cleanup
func (db *DB) Close() error {
	return db.file.Close()
}

// Put stores the value under given key
func (db *DB) Put(key, val []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}

	if val == nil {
		return ErrInvalidValue
	}

	db.m.Lock()
	defer db.m.Unlock()

	kve := newKVEntry(db.time.NowUnix(), key, val)

	err := db.rotateDataFile(int64(kve.header.entrySize()))
	if err != nil {
		return err
	}

	_, err = db.writeEntry(db.file, db.kd, kve)

	return err
}

func (db *DB) rotateDataFile(entrySz int64) error {
	if (db.file.Size() + entrySz) <= db.cfg.MaxDataFileSize {
		return nil
	}

	err := db.file.Close()
	if err != nil {
		return err
	}

	db.file, err = db.fs.Rotate(db.path)
	if err != nil {
		return err
	}

	db.kd.resetOffset()

	return nil
}

// Delete deletes a key/value pair if it exists or reports key not found
// error if the key does not exist
func (db *DB) Delete(key []byte) error {
	db.m.Lock()
	defer db.m.Unlock()

	_, err := db.get(key)
	if err != nil && !errors.Is(err, ErrCRCFailed) {
		return err
	}

	err = db.writeKeyVal(
		db.file,
		db.kd,
		newKVEntry(db.time.NowUnix(), nil, key),
	)
	if err != nil {
		return err
	}

	db.kd.unset(key)

	return nil
}

func (db *DB) writeEntry(file File, kd *keyDir, kve kvEntry) (*kdEntry, error) {
	err := db.writeKeyVal(file, kd, kve)
	if err != nil {
		return nil, err
	}

	return kd.set(kve.key, kve.header, file.Name()), nil
}

func (db *DB) writeKeyVal(file File, kd *keyDir, kve kvEntry) error {
	entry := kve.serialize()

	n, err := file.Write(entry)
	if err != nil {
		if n > 0 {
			kd.advanceOffsetBy(uint32(n))

			// TODO - What if entry has been written partially (in the middle or beginning of the file)
			// will the startup fail because the headers will not be correct (eg. headers might not be fully written
			// keys also and values
			// how do we mitigate this?

			return ErrPartialWrite
		}
	}

	return err
}

// Get retrieves a value stored under given key
func (db *DB) Get(key []byte) ([]byte, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	return db.get(key)
}

func (db *DB) get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrInvalidKey
	}

	ke, err := db.kd.get(key)
	if err != nil {
		return nil, err
	}

	val := make([]byte, ke.h.ValueSize)

	_, err = db.fs.ReadFileAt(db.path, ke.File, val, int64(ke.ValuePos))
	if err != nil {
		return nil, err
	}

	if ke.h.CRC != crc.CalcCRC32(val) {
		return nil, ErrCRCFailed
	}

	return val, nil
}

// Keys returns all keys
func (db *DB) Keys() []string {
	db.m.RLock()
	defer db.m.RUnlock()

	// TODO - Reuse mapFile maybe

	return db.kd.keys()
}

// TODO - Reuse mapFile for Fold feature

func (db *DB) isActive(file File) bool {
	return file.Name() == db.file.Name()
}
