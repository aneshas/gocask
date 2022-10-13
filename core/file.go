package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	// DataFileExt represents data file extension
	DataFileExt = ".csk"

	hintFilePartial = ".a"

	// HintFileExt represents hint file extension
	HintFileExt = ".a.csk"

	// TmpFileExt represents temp file extension
	TmpFileExt = ".tmp"
)

func mapEntries(file File, f func(kve kvEntry, name string, err error) error) error {
	return mapFile(file, func(r *bufio.Reader, name string) error {
		kve, err := parseKVEntry(r)
		if err != nil {
			return f(kvEntry{}, name, err)
		}

		if kve.isTombstone {
			return nil
		}

		return f(kve, name, nil)
	})
}

func mapFile(file File, f func(r *bufio.Reader, name string) error) error {
	var (
		r    = bufio.NewReader(file)
		name = file.Name()
	)

	for {
		err := f(r, name)

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("gocask: error mapping data file entries: %w", err)
		}
	}

	return nil
}

func isHintFile(file File) bool {
	return strings.Contains(file.Name(), hintFilePartial)
}
