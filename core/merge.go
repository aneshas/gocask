package core

import (
	"errors"
	"fmt"
	"log"
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

		if isHintFile(file) {
			return nil
		}

		merge := false

		if true {
			// TODO check threshold
			// either from keydir (eg. store stats per file)
			// or a pass through the file (probably inefficient)
			// does the first one slow the startup significantly?
			// less io is probably better
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
	mergedFile, kd, err := db.openTempMergeFile(file, merge)
	if err != nil {
		return err
	}

	hintFile, err := db.fs.OTruncate(db.path, fmt.Sprintf("%s.hint.tmp", file.Name()))
	if err != nil {
		return err
	}

	defer hintFile.Close()

	err = mapEntries(file, func(kve kvEntry, name string) error {
		// check crc and partial writes
		// get keydir

		kde, err := db.getKDEntry(kve.key)
		if err != nil {
			return err
		}

		if kde == nil {
			return nil
		}

		if mergedFile != nil {
			kde, err = db.writeEntry(mergedFile, kd, kve)
		}

		return db.writeHint(
			newHintEntry(kde.hintHeader(), kve.key),
			hintFile,
		)
	})

	return db.commitMerge(file.Name(), mergedFile, hintFile, kd)
}

func (db *DB) openTempMergeFile(file File, merge bool) (File, *keyDir, error) {
	if !merge {
		return nil, db.kd, nil
	}

	f, err := db.fs.OTruncate(db.path, fmt.Sprintf("%s.merge.tmp", file.Name()))
	if err != nil {
		return nil, nil, err
	}

	return f, newKeyDir(), nil
}

func (db *DB) getKDEntry(key []byte) (*kdEntry, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	entry, err := db.kd.get(key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil
		}

		return nil, err
	}

	return entry, nil
}

func (db *DB) writeHint(he hintEntry, file File) error {
	n, err := file.Write(he.serialize())
	if err != nil {
		if n > 0 {
			return ErrPartialWrite
		}

		return err
	}

	return nil
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

		db.kd.merge(kd)

		// TODO log kd merge
	}

	err := db.fs.Move(db.path, hf.Name(), fmt.Sprintf("%s%s", dest, hintFilePartial))
	if err != nil {
		// even if this errors out
		// new keydir has been merged, and we should still be in a valid state
		return err

		// TODO log hint
	}

	return nil
}
