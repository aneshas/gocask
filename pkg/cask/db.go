package cask

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"
)

// TODO - max file should be configurable (configure other stuff also per dependency and merge to one config in root)

var (
	// ErrKeyNotFound signifies that the requested key was not found in keydir
	// This effectively means that the key does not exist in the database itself
	ErrKeyNotFound = errors.New("gocask: key not found")

	// ErrPartialWrite signifies that the underlying disk write was not complete, which means
	// that write was successful but the entry is corrupted and the operation should be retried
	ErrPartialWrite = errors.New("gocask: key/value pair not fully written")
)

// InMemoryDB represents a magic value which can be used instead of db path
// in order to instantiate in memory file system instead of disk
const InMemoryDB = "in:mem:db"

// FS represents a file system interface
type FS interface {
	// Open should open the active data file for the given db path
	Open(string) (File, error)

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

// DB represents bitcask db implementation
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
	MaxDataFileSize: 1024 * 1024 * 1024 * 1024 * 10,
	//MaxDataFileSize: 1024 * 1024 * 40,
}

// Config represents gocask config
type Config struct {
	MaxDataFileSize int64
}

// NewDB instantiates new db with provided FS as storage mechanism
func NewDB(dbpath string, fs FS, time Time, cfg Config) (*DB, error) {
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

	return &caskDB, caskDB.init(f)
}

func (db *DB) init(activeFile File) error {
	return db.fs.Walk(db.path, func(file File) error {
		err := db.walkFile(file)
		if err != nil {
			return err
		}

		if file.Name() != activeFile.Name() {
			db.kd.resetOffset()
		}

		return nil
	})
}

func (db *DB) walkFile(file File) error {
	var (
		r    = bufio.NewReader(file)
		name = file.Name()
	)

	for {
		err := db.readEntry(r, name)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			// TODO - Test for ErrUnexpectedEOF and unknown err
			return fmt.Errorf("gocask: startup error: %w", err)
		}
	}

	return nil
}

func (db *DB) readEntry(r *bufio.Reader, file string) error {
	h, err := parseHeader(r)
	if err != nil {
		return err
	}

	keySize := h.KeySize

	if h.isTombstone() {
		keySize = h.ValueSize
	}

	key := make([]byte, keySize)

	_, err = io.ReadFull(r, key)
	if err != nil {
		return err
	}

	if h.isTombstone() {
		db.kd.unset(string(key))

		return nil
	}

	// TODO - Test
	_, err = r.Discard(int(h.ValueSize))
	if err != nil {
		return err
	}

	db.kd.set(string(key), h, file)

	return err
}

func (db *DB) Close() error {
	return db.file.Close()
}

// Put stores the value under given key
func (db *DB) Put(key, val []byte) error {
	db.m.Lock()
	defer db.m.Unlock()

	h := db.newHeader(key, val)

	if (db.file.Size() + int64(h.entrySize())) > db.cfg.MaxDataFileSize {
		err := db.file.Close()
		if err != nil {
			return err
		}

		file, err := db.fs.Rotate(db.path)
		if err != nil {
			return err
		}

		db.file = file

		db.kd.resetOffset()
	}

	h, err := db.writeKeyVal(h, key, val)
	if err != nil {
		return err
	}

	db.kd.set(string(key), h, db.file.Name())

	return nil
}

// Delete deletes a key/value pair if it exists or reports key not found
// error if the key does not exist
func (db *DB) Delete(key []byte) error {
	_, err := db.Get(key)
	if err != nil {
		return err
	}

	db.m.Lock()
	defer db.m.Unlock()

	h := db.newHeader(nil, key)

	_, err = db.writeKeyVal(h, nil, key)
	if err != nil {
		return err
	}

	db.kd.unset(string(key))

	return nil
}

func (db *DB) newHeader(key, val []byte) header {
	var kSize uint32

	if key != nil {
		kSize = uint32(len(key))
	}

	vSize := uint32(len(val))

	return newHeader(db.time.NowUnix(), kSize, vSize)
}

func (db *DB) writeKeyVal(h header, key, val []byte) (header, error) {
	entry := serializeEntry(h, key, val)

	n, err := db.file.Write(entry)
	if err != nil {
		if n > 0 {
			db.kd.advanceOffsetBy(uint32(n))

			return h, ErrPartialWrite
		}
	}

	return h, err
}

func serializeEntry(h header, key, val []byte) []byte {
	b := make([]byte, 0, int(headerSize)+len(key)+len(val))

	b = append(b, h.encode()...)

	if key != nil {
		b = append(b, key...)
	}

	b = append(b, val...)

	return b
}

// Get retrieves a value stored under given key
func (db *DB) Get(key []byte) ([]byte, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	ke, err := db.kd.get(string(key))
	if err != nil {
		return nil, err
	}

	val := make([]byte, ke.ValueSize)

	_, err = db.fs.ReadFileAt(db.path, ke.File, val, int64(ke.ValuePos))
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Keys returns all keys
func (db *DB) Keys() []string {
	db.m.RLock()
	defer db.m.RUnlock()

	return db.kd.keys()
}
