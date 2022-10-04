package core

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

// Merge will try to clean up data files by getting rid of deleted entries (if threshold is met).
// In addition to this it will also build a hint file for every data file in order to speed up the startup time.
// Each time merge is called it will try to merge the next file that has not been merged.
// Merge is fault-tolerant and will retain the database in a valid state even on merge/hint errors
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
			// log everything that's interesting
			// bubble up only unrecoverable errors

			log.Println(err)
		}

		done = true

		return nil
	})
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

	// TODO - We need to walk the file itself since this might cause data loss if some keys are deleted (if key is deleted
	// it is removed from the keydir)
	// TODO:
	// db. mapFile
	// get key from keydir
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

	return db.commitMerge(file.Name(), mergedFile, hintFile, kd)
}

func (db *DB) writeHint(key []byte, entry *kdEntry, file File) error {
	h := entry.h.toHint(entry.ValuePos)

	e := db.serializeHint(h, key)

	n, err := file.Write(e)
	if err != nil {
		if n > 0 {
			return ErrPartialWrite
		}

		return err
	}

	return nil
}

func (db *DB) serializeHint(h hintHeader, key []byte) []byte {
	b := make([]byte, 0, int(hintHeaderSize)+len(key))

	// reuse buffer for encoding (check / profile for other such optimisations)
	b = append(b, h.encode()...)

	return append(b, key...)
}

func (db *DB) commitMerge(dest string, mf, hf File, kd *keyDir) error {
	db.m.Lock()
	defer db.m.Unlock()

	if mf != nil {
		err := db.fs.Move(db.path, mf.Name(), dest)
		if err != nil {
			return err
		}

		// TODO log merge
	}

	db.kd.merge(kd)

	err := db.fs.Move(db.path, hf.Name(), fmt.Sprintf("%s.a", dest))
	if err != nil {
		// even if this errors out
		// new keydir has been merged, and we should still be in a valid state
		return err

		// TODO log hint
	}

	return nil
}
