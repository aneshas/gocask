package cask_test

import (
	"fmt"
	"github.com/aneshas/gocask/internal/cask"
	"github.com/aneshas/gocask/internal/cask/testutil"
	caskfs "github.com/aneshas/gocask/internal/fs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShould_Successfully_Store_Values(t *testing.T) {
	cases := []struct {
		key string
		val string
		now uint32
	}{
		{
			key: "foo",
			val: "foo bar baz",
			now: 1234,
		},
		{
			key: "name",
			val: "john doe",
			now: 443,
		},
		{
			key: "1234",
			val: `{"foo": "bar"}`,
			now: 34389,
		},
		{
			key: "foo bar baz",
			val: "test",
			now: 999999,
		},
	}

	fs := testutil.NewFS().WithWriteSupport()

	for _, tc := range cases {
		t.Run(fmt.Sprintf("put %s", tc.key), func(t *testing.T) {
			db, err := cask.NewDB(fs.Path, fs, testutil.Time(tc.now))

			assert.NoError(t, err)

			key := []byte(tc.key)
			val := []byte(tc.val)

			err = db.Put(key, val)

			assert.NoError(t, err)

			fs.VerifyEntryWritten(t, testutil.Entry(tc.now, key, val))

			assert.NoError(t, db.Close())
		})
	}
}

// TODO Error values should live in the root

// TODO Errors cases
// file open errors
// path errors (probably propagations)
// non matching write (len, err)
// allow Empty val
// key not found
// empty key
// nil val
// nil key
// same key behavior
// all read, write, seek errors that return different number of bytes since this can lead to corrupt state

func TestShould_Fetch_Previously_Saved_Value(t *testing.T) {
	cases := []struct {
		key string
		val string
		now uint32
	}{
		{
			key: "foo",
			val: "foo bar baz",
			now: 1234,
		},
		{
			key: "name",
			val: "john doe",
			now: 443,
		},
		{
			key: "1234",
			val: `{"foo": "bar"}`,
			now: 34389,
		},
		{
			key: "foo bar baz",
			val: "test",
			now: 999999,
		},
	}

	fs := caskfs.NewInMemory()

	for _, tc := range cases {
		t.Run(fmt.Sprintf("get/put %s", tc.key), func(t *testing.T) {
			key := []byte(tc.key)
			val := []byte(tc.val)

			db, err := cask.NewDB("path/to/db", fs, testutil.Time(tc.now))

			assert.NoError(t, err)

			err = db.Put(key, val)

			assert.NoError(t, err)

			got, err := db.Get(key)

			assert.NoError(t, err)
			assert.Equal(t, val, got)
		})
	}
}

func TestShould_Fetch_Existing_Values_After_Startup(t *testing.T) {
	seed := []struct {
		file string
		key  string
		val  string
		now  uint32
	}{
		{
			file: "data",
			key:  "foo",
			val:  "foo bar baz",
			now:  1234,
		},
		{
			file: "data",
			key:  "name",
			val:  "john doe",
			now:  443,
		},
		{
			file: "data01",
			key:  "foo1",
			val:  "foo bar baz",
			now:  1234,
		},
		{
			file: "data01",
			key:  "name1",
			val:  "john doe",
			now:  443,
		},
		{
			file: "data02",
			key:  "1234",
			val:  `{"foo": "bar"}`,
			now:  34389,
		},
		{
			file: "data03",
			key:  "foo bar baz",
			val:  "test",
			now:  999999,
		},
		{
			file: "data03",
			key:  "foo bar baz 01",
			val:  "test",
			now:  999999,
		},
		{
			file: "data03",
			key:  "foo2",
			val:  "test foo bar",
			now:  200,
		},
		{
			file: "data03",
			key:  "foo bar baz 02",
			val:  "test",
			now:  999999,
		},
		{
			file: "data03",
			key:  "baz",
			val:  "test",
			now:  999999,
		},
	}

	cases := []struct {
		key string
		val string
	}{
		{
			key: "foo",
			val: "foo bar baz",
		},
		{
			key: "name",
			val: "john doe",
		},
		{
			key: "foo1",
			val: "foo bar baz",
		},
		{
			key: "name1",
			val: "john doe",
		},
		{
			key: "1234",
			val: `{"foo": "bar"}`,
		},
		{
			key: "foo bar baz",
			val: "test",
		},
		{
			key: "foo bar baz 01",
			val: "test",
		},
		{
			key: "foo2",
			val: "test foo bar",
		},
		{
			key: "foo bar baz 02",
			val: "test",
		},
		{
			key: "baz",
			val: "test",
		},
	}

	fs := testutil.NewFS().WithMockDataFiles()

	for _, tc := range seed {
		fs.AddDataFileEntry(
			tc.file,
			testutil.Entry(tc.now, []byte(tc.key), []byte(tc.val)),
		)
	}

	var time testutil.Time

	db, err := cask.NewDB(fs.Path, fs, time)

	for _, tc := range cases {
		t.Run(fmt.Sprintf("get %s", tc.key), func(t *testing.T) {
			key := []byte(tc.key)
			val := []byte(tc.val)

			assert.NoError(t, err)

			got, err := db.Get(key)

			assert.NoError(t, err)
			assert.Equal(t, val, got)
		})
	}
}

func TestShould_Fetch_Updated_Values_From_Different_Files(t *testing.T) {
	seed := []struct {
		file string
		key  string
		val  string
		now  uint32
	}{
		{
			file: "data",
			key:  "foo",
			val:  "foo bar baz",
			now:  1234,
		},
		{
			file: "data",
			key:  "bar",
			val:  "foo bar baz",
			now:  1234,
		},
		{
			file: "data01",
			key:  "foo",
			val:  "john doe overwrites you",
			now:  443,
		},
		{
			file: "data02",
			key:  "bar",
			val:  "foo bar buzzed",
			now:  1234,
		},
	}

	cases := []struct {
		key string
		val string
	}{
		{
			key: "foo",
			val: "john doe overwrites you",
		},
		{
			key: "bar",
			val: "foo bar buzzed",
		},
	}

	fs := testutil.NewFS().WithMockDataFiles()

	for _, tc := range seed {
		fs.AddDataFileEntry(
			tc.file,
			testutil.Entry(tc.now, []byte(tc.key), []byte(tc.val)),
		)
	}

	var time testutil.Time

	db, err := cask.NewDB(fs.Path, fs, time)

	for _, tc := range cases {
		t.Run(fmt.Sprintf("get %s", tc.key), func(t *testing.T) {
			key := []byte(tc.key)
			val := []byte(tc.val)

			assert.NoError(t, err)

			got, err := db.Get(key)

			assert.NoError(t, err)
			assert.Equal(t, val, got)
		})
	}
}