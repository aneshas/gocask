package core

import (
	"errors"
	"fmt"
)

// Merge will try to clean up data files by getting rid of deleted entries (if threshold is met).
// In addition to this it will also build a hint file for every data file in order to speed up the startup time.
// Each time merge is called it will try to merge the next file that has not yet been merged.
// Merge is fault-tolerant and will retain the database in a valid state even on merge/hint errors
// Merging is a potentially relatively expensive operation and should be called strategically (eg. not
// in high-load/high-usage situations. This is why the scheduling is left to the caller.
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

		// TODO use logrus
		// log everything that's interesting

		err := db.mergeAndHint(file, merge)
		if err != nil {
			// log
		}

		done = true

		return err
	})
}

func (db *DB) mergeAndHint(file File, merge bool) error {
	mergeF, kd, err := db.openMerge(file, merge)
	if err != nil {
		return err
	}

	hintF, err := db.openHint(file)
	if err != nil {
		return err
	}

	// TODO Log all deferred errs (package-wide)
	// log however you like, only leave logging destination configurable
	defer hintF.Close()

	err = mapEntries(file, func(kve kvEntry, name string, err error) error {
		if err != nil {
			if errors.Is(err, ErrCRCFailed) {
				return nil
			}

			return err
		}

		kde, err := db.getKDEntry(kve.key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				return nil
			}

			return err
		}

		if mergeF != nil {
			kde, err = db.writeEntry(mergeF, kd, kve)
		}

		return db.writeHint(
			newHintEntry(kde.hintHeader(), kve.key),
			hintF,
		)
	})

	return db.commitMerge(file.Name(), mergeF, hintF, kd)
}

func (db *DB) openMerge(file File, merge bool) (File, *keyDir, error) {
	if !merge {
		return nil, db.kd, nil
	}

	f, err := db.openTmpFile(file.Name(), "merge")
	if err != nil {
		return nil, nil, err
	}

	return f, newKeyDir(), nil
}

func (db *DB) openHint(file File) (File, error) {
	return db.openTmpFile(file.Name(), "hint")
}

func (db *DB) openTmpFile(file string, postfix string) (File, error) {
	return db.fs.OTruncate(db.path, fmt.Sprintf("%s.%s%s", file, postfix, TmpFileExt))
}

func (db *DB) getKDEntry(key []byte) (*kdEntry, error) {
	db.m.RLock()
	defer db.m.RUnlock()

	entry, err := db.kd.get(key)
	if err != nil {
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
