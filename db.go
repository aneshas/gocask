package gocask

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"
)

// TODO - make sure max file size does not exceed uint32 length
// TODO - write in pages ? (this would be handled in store abstraction)
// TODO - NewTestDB()

func Open(db string) (*DB, error) {
	currDataFile := "data.csk"

	err := createDB(db)
	if err != nil {
		return nil, err
	}

	dataFile := fmt.Sprintf("%s/%s", db, currDataFile)

	// abstract these fs operations behind io.FS ?
	// or behind our own FS abstraction
	file, err := os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return nil, fmt.Errorf("could not open db: %w", err)
	}

	caskDB := &DB{
		store: fileStore{
			file,
		},
		kd:           newKeyDir(),
		currDataFile: currDataFile,
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

		return os.Mkdir(db, 0755)
	}

	if !info.IsDir() {
		return fmt.Errorf("db exists and it's not a folder")
	}

	return nil
}

type store interface {
	io.ReaderAt
	io.ReadWriteSeeker
	io.Closer

	Name() string
}

type fileStore struct {
	*os.File
}

func (fs fileStore) Name() string {
	return filepath.Base(fs.File.Name())
}

type DB struct {
	store        store
	kd           *keyDir
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

func (db *DB) walkFile(file store) error {
	for {
		err := db.readEntry(file)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
	}

	fmt.Printf("%#v", db.kd)

	return nil
}

func (db *DB) readEntry(file store) error {
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
	return db.store.Close()
}

func (db *DB) Put(key string, value []byte) error {
	t := uint32(time.Now().UTC().Unix())
	kSize := uint32(len([]byte(key)))
	vSize := uint32(len(value))

	h := newHeader(t, kSize, vSize)

	encoded, err := serializeEntry(h, []byte(key), value)
	if err != nil {
		return err
	}

	_, err = db.store.Write(encoded)

	db.kd.Set(key, h, db.currDataFile)

	return err
}

func serializeEntry(h header, key, val []byte) ([]byte, error) {
	var buff bytes.Buffer

	_, err := buff.Write(h.Encode())
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(key)
	if err != nil {
		return nil, err
	}

	_, err = buff.Write(val)

	return buff.Bytes(), err
}

// TODO - Change key to []byte

func (db *DB) Get(key string) ([]byte, error) {
	ke, err := db.kd.Get(key)
	if err != nil {
		return nil, err
	}

	val := make([]byte, ke.ValueSize)

	// TODO account for ke.File
	_, err = db.store.ReadAt(val, int64(ke.ValuePos))
	if err != nil {
		return nil, err
	}

	return val, nil
}
