package gocask

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"
)

func Open(db string) (*DB, error) {
	currDataFile := "data"

	createDB(db)

	dataFile := fmt.Sprintf("%s/%s.csk", db, currDataFile)

	file, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not open db: %w", err)
	}

	caskDB := &DB{
		dataFile:     file,
		keydir:       newKeydir(),
		currDataFile: dataFile,
		db:           db,
	}

	return caskDB, caskDB.init()
}

func createDB(db string) error {
	info, err := os.Stat(db)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	if !info.IsDir() {
		return fmt.Errorf("db exists and it's not a folder")
	}

	return os.Mkdir(db, 0755)
}

type DB struct {
	dataFile     *os.File
	keydir       *keydir
	currDataFile string
	db           string
}

func (db *DB) init() error {
	entries, err := os.ReadDir(db.db)
	if err != nil {
		return err
	}

	for _, e := range entries {
		file, err := os.OpenFile(path.Join(db.db, e.Name()), os.O_RDONLY, 0755)
		if err != nil {
			return err
		}

		err = db.walkFile(file)
		if err != nil {
			return err
		}

		err = file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) walkFile(file *os.File) error {
	var offset int64

	h := emptyHeader()

	for {
		esz, err := db.readEntry(h, file, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
		}

		offset += esz
	}

	return nil
}

func (db *DB) readEntry(h header, file *os.File, offset int64) (int64, error) {
	_, err := file.ReadAt(h, offset)
	if err != nil {
		return 0, err
	}

	t, ksz, vsz, err := h.Parse()
	if err != nil {
		return 0, err
	}

	key := make([]byte, ksz)

	_, err = file.ReadAt(key, offset+12)
	if err != nil {
		return 0, err
	}

	esz := int64(12 + ksz + vsz)

	db.keydir.Set(string(key), esz, vsz, t, file.Name())

	return esz, nil
}

func (db *DB) Close() error {
	return db.dataFile.Close()
}

func (db *DB) Put(key string, value []byte) error {
	t := int32(time.Now().UTC().Unix())
	kSize := int32(len([]byte(key)))
	vSize := int32(len(value))

	h := newHeader(t, kSize, vSize)

	var buff bytes.Buffer

	_, err := buff.Write(h)
	if err != nil {
		return err
	}

	_, err = buff.WriteString(key)
	if err != nil {
		return err
	}

	_, err = buff.Write(value)
	if err != nil {
		return err
	}

	entry := buff.Bytes()

	_, err = db.dataFile.Write(entry)

	db.keydir.Set(key, int64(len(entry)), vSize, t, db.currDataFile)

	return err
}

func (db *DB) Get(key string) ([]byte, error) {
	ke, err := db.keydir.Get(key)
	if err != nil {
		return nil, err
	}

	val := make([]byte, ke.ValueSize)

	// TODO account for ke.File
	_, err = db.dataFile.ReadAt(val, ke.ValuePos)
	if err != nil {
		return nil, err
	}

	return val, nil
}
