package cask_test

import (
	"errors"
	"fmt"
	caskfs "github.com/aneshas/gocask/internal/fs"
	"github.com/aneshas/gocask/pkg/cask"
	"github.com/aneshas/gocask/pkg/cask/testutil"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"sort"
	"testing"
	"time"
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

func TestShould_Fetch_Previously_Saved_Value_In_Memry(t *testing.T) {
	saveAndFetch(t, "", caskfs.NewInMemory())
}

func TestShould_Fetch_Previously_Saved_Value_On_Disk(t *testing.T) {
	dbName := fmt.Sprintf("gocask_db_%d", time.Now().Unix())
	dbPath := path.Join(os.TempDir(), dbName)

	saveAndFetch(t, dbPath, caskfs.NewDisk())

	assert.NoError(t, os.RemoveAll(dbPath))
}

func saveAndFetch(t *testing.T, dbPath string, fs cask.FS) {
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

	for _, tc := range cases {
		t.Run(fmt.Sprintf("get/put %s", tc.key), func(t *testing.T) {
			key := []byte(tc.key)
			val := []byte(tc.val)

			db, _ := cask.NewDB(dbPath, fs, testutil.Time(tc.now))

			err := db.Put(key, val)

			assert.NoError(t, err)

			got, err := db.Get(key)

			assert.NoError(t, err)
			assert.Equal(t, val, got)

			err = db.Close()

			assert.NoError(t, err)
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
			file: "data0",
			key:  "foo",
			val:  "foo bar baz",
			now:  1234,
		},
		{
			file: "data0",
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

	fs := testutil.NewFS().UseMockDataFiles()

	for _, tc := range seed {
		fs.AddMockDataFileEntry(
			tc.file,
			testutil.Entry(tc.now, []byte(tc.key), []byte(tc.val)),
		)
	}

	var time testutil.Time

	db, _ := cask.NewDB(fs.Path, fs, time)

	for _, tc := range cases {
		t.Run(fmt.Sprintf("get %s", tc.key), func(t *testing.T) {
			key := []byte(tc.key)
			val := []byte(tc.val)

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
			file: "data0",
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

	fs := testutil.NewFS().UseMockDataFiles()

	for _, tc := range seed {
		fs.AddMockDataFileEntry(
			tc.file,
			testutil.Entry(tc.now, []byte(tc.key), []byte(tc.val)),
		)
	}

	var time testutil.Time

	db, _ := cask.NewDB(fs.Path, fs, time)

	for _, tc := range cases {
		t.Run(fmt.Sprintf("get %s", tc.key), func(t *testing.T) {
			key := []byte(tc.key)
			val := []byte(tc.val)

			got, err := db.Get(key)

			assert.NoError(t, err)
			assert.Equal(t, val, got)
		})
	}
}

func TestShould_Not_Be_Able_To_Retrieve_Deleted_Key(t *testing.T) {
	fs := caskfs.NewInMemory()

	var time testutil.Time

	db, _ := cask.NewDB("", fs, time)

	key := []byte("foo")
	val := []byte("bar")

	_ = db.Put(key, val)

	err := db.Delete(key)

	assert.NoError(t, err)

	_, err = db.Get(key)

	assert.ErrorIs(t, err, cask.ErrKeyNotFound)
}

func TestShould_Not_Be_Able_To_Retrieve_Deleted_Key_After_Startup(t *testing.T) {
	fs := caskfs.NewInMemory()

	var time testutil.Time

	db, _ := cask.NewDB("", fs, time)

	key := []byte("foo")
	val := []byte("bar")

	_ = db.Put(key, val)
	_ = db.Delete(key)

	db, _ = cask.NewDB("", fs, time)

	_, err := db.Get(key)

	assert.ErrorIs(t, err, cask.ErrKeyNotFound)
}

func TestShould_Be_Able_To_Reset_Deleted_Key(t *testing.T) {
	fs := caskfs.NewInMemory()

	var time testutil.Time

	db, _ := cask.NewDB("", fs, time)

	key := []byte("foo")
	val := []byte("bar")
	newVal := []byte("baz")

	_ = db.Put(key, val)
	_ = db.Delete(key)
	_ = db.Put(key, newVal)

	got, err := db.Get(key)

	assert.NoError(t, err)
	assert.Equal(t, newVal, got)
}

func TestShould_Report_KeyNotFound_When_Deleting_Non_Existent_Key(t *testing.T) {
	fs := caskfs.NewInMemory()

	var time testutil.Time

	db, _ := cask.NewDB("", fs, time)

	err := db.Delete([]byte("i-dont-exist"))

	assert.ErrorIs(t, err, cask.ErrKeyNotFound)
}

func TestShould_Fetch_All_Keys(t *testing.T) {
	seed := []struct {
		file string
		key  string
	}{
		{
			file: "data",
			key:  "foo",
		},
		{
			file: "data",
			key:  "bar",
		},
		{
			file: "data01",
			key:  "foobar",
		},
		{
			file: "data02",
			key:  "baz",
		},
	}

	fs := testutil.NewFS().UseMockDataFiles()

	for _, tc := range seed {
		fs.AddMockDataFileEntry(
			tc.file,
			testutil.Entry(123, []byte(tc.key), []byte("val")),
		)
	}

	var time testutil.Time

	db, _ := cask.NewDB(fs.Path, fs, time)

	wantKeys := []string{"foo", "bar", "foobar", "baz"}
	gotKeys := db.Keys()

	sort.Strings(gotKeys)
	sort.Strings(wantKeys)

	assert.Equal(t, wantKeys, gotKeys)
}

func TestShould_Return_Empty_Keys_Slice_For_Empty_DB(t *testing.T) {
	var time testutil.Time

	db, _ := cask.NewDB("", caskfs.NewInMemory(), time)

	assert.Equal(t, []string{}, db.Keys())
}

func TestPut_Should_Report_Failed_Write_Error(t *testing.T) {
	wantErr := errors.New("an error")

	fs := testutil.NewFS().WithFailWithErrOnWrite(wantErr)

	var time testutil.Time

	db, _ := cask.NewDB(fs.Path, fs, time)

	err := db.Put([]byte("key"), []byte("val"))

	assert.ErrorIs(t, err, err)
}

func TestPut_Should_Report_Typed_Key_Not_Found_Error(t *testing.T) {
	fs := testutil.NewFS().WithWriteSupport()

	var time testutil.Time

	db, _ := cask.NewDB(fs.Path, fs, time)

	_, err := db.Get([]byte("foo"))

	assert.ErrorIs(t, err, cask.ErrKeyNotFound)
}

func TestPut_Should_Report_File_Read_Error_For_Existing_Key(t *testing.T) {
	key := []byte("foo")
	wantErr := errors.New("an error")

	fs := testutil.NewFS().
		WithFailOnReadValueFromFile(wantErr).
		UseMockDataFiles()

	fs.AddMockDataFileEntry(
		"data",
		testutil.Entry(123, key, []byte("value")),
	)

	var time testutil.Time

	db, _ := cask.NewDB(fs.Path, fs, time)

	_, err := db.Get(key)

	assert.ErrorIs(t, err, wantErr)
}

func TestShould_Tolerate_Partial_Write_On_Put(t *testing.T) {
	var time testutil.Time

	key := []byte("key")
	val := []byte("foobarbaz")

	path := "mydb"

	fs := testutil.
		NewInMemory(caskfs.NewInMemory()).
		WithPartialWriteFor(key)

	db, _ := cask.NewDB(path, fs, time)

	err := db.Put([]byte("user"), []byte("user123456"))

	assert.NoError(t, err)

	err = db.Put(key, val)

	assert.ErrorIs(t, err, cask.ErrPartialWrite)

	wantKey := []byte("ishould")
	wantVal := []byte("befine")

	err = db.Put(wantKey, wantVal)

	assert.NoError(t, err)

	gotVal, err := db.Get(wantKey)

	assert.NoError(t, err)
	assert.Equal(t, wantVal, gotVal)
}

func TestShould_Tolerate_Partial_Write_On_Delete(t *testing.T) {
	var time testutil.Time

	key := []byte("key")
	val := []byte("foobarbaz")

	path := "mydb"

	inMem := caskfs.NewInMemory()

	db, err := cask.NewDB(path, inMem, time)

	assert.NoError(t, err)

	err = db.Put(key, val)

	assert.NoError(t, err)

	db, _ = cask.NewDB(
		path,
		testutil.
			NewInMemory(inMem).
			WithPartialWriteFor(key),
		time,
	)

	err = db.Delete(key)

	assert.ErrorIs(t, err, cask.ErrPartialWrite)

	wantKey := []byte("ishould")
	wantVal := []byte("befine")

	err = db.Put(wantKey, wantVal)

	assert.NoError(t, err)

	gotVal, err := db.Get(wantKey)

	assert.NoError(t, err)
	assert.Equal(t, wantVal, gotVal)
}

// TODO Errors cases

// Corrupt header error
// CRC errors
// file open errors
// allow Empty val get
// empty key put not allow
// nil val
// nil key
// same key behavior
