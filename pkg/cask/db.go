package cask

import (
	"bytes"
	"errors"
	"io"
)

// TODO - max file should be configurable (configure other stuff also per dependency and merge to one config in root)
// TODO - Add namespaces

var (
	// ErrKeyNotFound signifies that the requested key was not found in keydir
	// This effectively means that the key does not exist in the database itself
	ErrKeyNotFound = errors.New("caskdb: key not found")
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

		db.kd.ResetOffset()

		return nil
	})
}

func (db *DB) walkFile(file File) error {
	for {
		err := db.readEntry(file)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
	}

	return nil
}

func (db *DB) readEntry(file File) error {
	h, err := parseHeader(file)
	if err != nil {
		return err
	}

	key := make([]byte, h.KeySize)

	_, err = file.Read(key)
	if err != nil {
		return err
	}

	db.kd.Set(string(key), h, file.Name())

	_, err = file.Seek(int64(h.ValueSize), 1)

	return err
}

func (db *DB) Close() error {
	return db.file.Close()
}

// Put stores the value under given key
func (db *DB) Put(key, value []byte) error {
	kSize := uint32(len(key))
	vSize := uint32(len(value))

	h := newHeader(db.time.NowUnix(), kSize, vSize)

	_, err := db.file.Write(serializeEntry(h, key, value))

	db.kd.Set(string(key), h, db.file.Name())

	return err
}

func serializeEntry(h header, key, val []byte) []byte {
	var buff bytes.Buffer

	buff.Write(h.encode())
	buff.Write(key)
	buff.Write(val)

	return buff.Bytes()
}

// Get retrieves a value stored under given key
func (db *DB) Get(key []byte) ([]byte, error) {
	ke, err := db.kd.Get(string(key))
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
