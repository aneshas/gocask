package cask

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// TODO - max file should be configurable (configure other stuff also per dependency and merge to one config in root)

var (
	// ErrKeyNotFound signifies that the requested key was not found in keydir
	// This effectively means that the key does not exist in the database itself
	ErrKeyNotFound = errors.New("gocask: key not found")
)

// InMemoryDB represents a magic value which can be used instead of db path
// in order to instantiate in memory file system instead of disk
const InMemoryDB = "in:mem:db"

// FS represents a file system interface
type FS interface {
	// Open should open the active data file for a given db path
	Open(string) (File, error)

	// Walk should walk through all data files for a given db path
	Walk(string, func(File) error) error

	// ReadFileAt should read a chunk of named path data file at a given offset
	ReadFileAt(string, string, []byte, int64) (int, error)
}

// File represents a single fs data file
type File interface {
	io.ReadWriteSeeker
	io.Closer

	Name() string
}

// Time represents time provider
type Time interface {
	NowUnix() uint32
}

// DB represents bitcask db implementation
type DB struct {
	time Time
	fs   FS
	file File
	path string
	kd   *keyDir
}

// NewDB instantiates new db with provided FS as storage mechanism
func NewDB(dbpath string, fs FS, time Time) (*DB, error) {
	f, err := fs.Open(dbpath)
	if err != nil {
		return nil, err
	}

	caskDB := DB{
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
		err := db.walkFile(file)
		if err != nil {
			return err
		}

		db.kd.resetOffset()

		return nil
	})
}

func (db *DB) walkFile(file File) error {
	r := bufio.NewReader(file)
	name := file.Name()

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
	h, err := db.writeKeyVal(key, val)
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

	_, err = db.writeKeyVal(nil, key)
	if err != nil {
		return err
	}

	db.kd.unset(string(key))

	return nil
}

// Get retrieves a value stored under given key
func (db *DB) Get(key []byte) ([]byte, error) {
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

func (db *DB) writeKeyVal(key, val []byte) (header, error) {
	var kSize uint32

	if key != nil {
		kSize = uint32(len(key))
	}

	vSize := uint32(len(val))

	h := newHeader(db.time.NowUnix(), kSize, vSize)

	_, err := db.file.Write(serializeEntry(h, key, val))

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

// Keys returns all keys
func (db *DB) Keys() []string {
	return db.kd.keys()
}
