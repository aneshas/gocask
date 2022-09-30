package core

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/aneshas/gocask/internal/crc"
	"io"
	"log"
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

	// DataFileExt represents data file extension
	DataFileExt = ".csk"

	// HintFileExt represents hint file extension
	HintFileExt = ".a.csk"

	// TmpFileExt represents temp file extension
	TmpFileExt = ".tmp"
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
		err := db.walkFile(file)
		if err != nil {
			return err
		}

		if !db.isActive(file) {
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
		var err error

		if strings.Contains(file.Name(), ".a") {
			err = db.readHintEntry(r, name)
		} else {
			err = db.readEntry(r, name)
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("gocask: startup error: %w", err)
		}
	}

	return nil
}

func (db *DB) readHintEntry(r *bufio.Reader, file string) error {
	h, err := parseHintHeader(r)
	if err != nil {
		return err
	}

	key := make([]byte, h.KeySize)

	_, err = io.ReadFull(r, key)
	if err != nil {
		return err
	}

	db.kd.setFromHint(key, h, strings.TrimSuffix(file, ".a"))

	return err
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
		db.kd.unset(key)

		return nil
	}

	_, err = r.Discard(int(h.ValueSize))
	if err != nil {
		return err
	}

	db.kd.set(key, h, file)

	return err
}

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

	h := newKVHeader(db.time.NowUnix(), key, val)

	err := db.rotateDataFile(int64(h.entrySize()))
	if err != nil {
		return err
	}

	err = db.writeKeyVal(db.file, db.kd, h, key, val)
	if err != nil {
		return err
	}

	db.kd.set(key, h, db.file.Name())

	return nil
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

	h := newKVHeader(db.time.NowUnix(), nil, key)

	err = db.writeKeyVal(db.file, db.kd, h, nil, key)
	if err != nil {
		return err
	}

	db.kd.unset(key)

	return nil
}

func (db *DB) writeKeyVal(file File, kd *keyDir, h header, key, val []byte) error {
	entry := serializeEntry(h, key, val)

	n, err := file.Write(entry)
	if err != nil {
		if n > 0 {
			kd.advanceOffsetBy(uint32(n))

			return ErrPartialWrite
		}
	}

	return err
}

func (db *DB) Merge() error {
	done := false

	return db.fs.Walk(db.path, func(file File) error {
		if done || db.isActive(file) {
			return nil
		}

		// TODO extensions as constants and add helper methods here
		if strings.Contains(file.Name(), ".a") {
			return nil
		}

		merge := false

		if true {
			// TODO check threshold
			merge = true
		}

		err := db.mergeAndHint(file, merge)
		if err != nil {
			// TODO use logrus
			log.Println(err)
		}

		done = true

		return nil
	})
}

func (db *DB) isActive(file File) bool {
	return file.Name() == db.file.Name()
}

func (db *DB) mergeAndHint(file File, merge bool) error {
	var (
		mergedFile File
		kd         = db.kd
	)

	if merge {
		kd = newKeyDir()

		f, err := db.fs.OTruncate(db.path, fmt.Sprintf("%s.merge.tmp", file.Name()))
		if err != nil {
			return err
		}

		mergedFile = f

		defer mergedFile.Close()
	}

	hintFile, err := db.fs.OTruncate(db.path, fmt.Sprintf("%s.hint.tmp", file.Name()))
	if err != nil {
		return err
	}

	defer hintFile.Close()

	err = db.kd.mapEntries(file.Name(), func(key []byte, entry *kdEntry) error {
		val, err := db.get(key)
		if err != nil {
			if errors.Is(err, ErrCRCFailed) {
				return nil
			}

			return err
		}

		if mergedFile != nil {
			err = db.writeKeyVal(mergedFile, kd, entry.h, key, val)
			if err != nil {
				return err
			}

			entry = kd.set(key, entry.h, file.Name())
		}

		return db.writeHint(key, entry, hintFile)
	})
	if err != nil {
		// maybe try to clean up the temp files
		return err
	}

	db.m.Lock()
	defer db.m.Unlock()

	if mergedFile != nil {
		err = db.fs.Move(db.path, mergedFile.Name(), file.Name())
		if err != nil {
			return err
		}
	}

	db.kd.merge(kd)

	err = db.fs.Move(db.path, hintFile.Name(), fmt.Sprintf("%s.a", file.Name()))
	if err != nil {
		// even if this errors out
		// keydir has been merged and we should still be in a valid state
		return err
	}

	return nil
}

func (db *DB) writeHint(key []byte, entry *kdEntry, file File) error {
	h := entry.h.toHint(entry.ValuePos)

	e := serializeHint(h, key)

	n, err := file.Write(e)
	if err != nil {
		if n > 0 {
			return ErrPartialWrite
		}

		return err
	}

	return nil
}

func serializeEntry(h header, key, val []byte) []byte {
	b := make([]byte, 0, int(headerSize)+len(key)+len(val))

	// reuse buffer for encoding (check / profile for other such optimisations)
	b = append(b, h.encode()...)

	if key != nil {
		b = append(b, key...)
	}

	return append(b, val...)
}

func serializeHint(h hintHeader, key []byte) []byte {
	b := make([]byte, 0, int(hintHeaderSize)+len(key))

	// reuse buffer for encoding (check / profile for other such optimisations)
	b = append(b, h.encode()...)

	return append(b, key...)
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

	return db.kd.keys()
}
