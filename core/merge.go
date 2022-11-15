package core

import (
	"errors"
	"fmt"
	"log"
)

// Merge will try to clean up data files by getting rid of deleted entries (if threshold is met).
// In addition to this it will also build a hint file for every data file in order to speed up the startup time.
// Each time merge is called it will try to merge the next file that has not yet been merged.
// Merge is fault-tolerant and will retain the database in a valid state even on merge/hint errors
// Merging is a potentially relatively expensive operation and should be called strategically (eg. not
// in high-load/high-usage situations. This is why the scheduling is left to the caller.
func (db *DB) Merge() error {
	// TODO - What to do with overriden keys (consider towards
	// the threshold for a given file)
	// TODO start with simple threshold (if any deleted or updated keys)

	// build the stats during keydir startup but keep it updated
	// after put and delete operations

	if db.merging {
		return nil
	}

	defer func() {
		db.merging = false
	}()

	return db.fs.Walk(db.path, func(file File) error {
		if db.isActive(file) || isHintFile(file) {
			return nil
		}

		merge := true
		// TODO check threshold

		// TODO use logrus
		// log everything that's interesting
		log.Println("merging ", file.Name())

		err := db.mergeAndHint(file, merge)
		if err != nil {
			// log
			log.Println("error merging ", err)
		}

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

	if err := mapEntries(file, func(kve kvEntry, name string, err error) error {
		if err != nil {
			if errors.Is(err, ErrCRCFailed) {
				return nil
			}

			return err
		}

		kde, err := db.getKDEntry(kve.key)
		if err != nil {
			return nil
		}

		if mergeF != nil {
			// TODO Err not used
			kde, err = db.writeEntry(mergeF, kd, kve)
			if err != nil {
				return nil
			}
		}

		err = db.writeHint(
			newHintEntry(kde.hintHeader(), kve.key),
			hintF,
		)
		if err != nil {
			return err
		}

		db.kd.unTaint(file.Name())

		return nil
	}); err != nil {
		return err
	}

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

		log.Println("merged ", dest)

		db.kd.merge(kd)

		log.Println("merged keydir for", dest)
	}

	err := db.fs.Move(db.path, hf.Name(), fmt.Sprintf("%s%s", dest, hintFilePartial))
	if err != nil {
		// even if this errors out
		// new keydir has been merged, and we should still be in a valid state
		return err
	}

	log.Println("created hint file for ", dest)

	return nil
}
