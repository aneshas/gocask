package core_test

import (
	"errors"
	"fmt"
	"github.com/aneshas/gocask/core"
	"github.com/aneshas/gocask/core/testutil"
	caskfs "github.com/aneshas/gocask/internal/fs"
	"github.com/stretchr/testify/assert"
	"os"
	gopath "path"
	"sort"
	"testing"
	gotime "time"
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

	fs := testutil.NewFS().WithMockWriteSupport()

	for _, tc := range cases {
		t.Run(fmt.Sprintf("put %s", tc.key), func(t *testing.T) {
			db, err := core.NewDB(fs.Path, fs, testutil.Time(tc.now), core.DefaultConfig)

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
	dbName := fmt.Sprintf("gocask_db_%d", gotime.Now().Unix())
	dbPath := gopath.Join(os.TempDir(), dbName)

	defer os.RemoveAll(dbPath)

	saveAndFetch(t, dbName, caskfs.NewDisk(core.GoTime{}))
}

func saveAndFetch(t *testing.T, dbPath string, fs core.FS) {
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
		{
			key: "emptyval",
			val: "",
			now: 88888,
		},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("get/put %s", tc.key), func(t *testing.T) {
			key := []byte(tc.key)
			val := []byte(tc.val)

			config := core.DefaultConfig

			config.DataDir = os.TempDir()

			db, _ := core.NewDB(dbPath, fs, testutil.Time(tc.now), config)

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

	db, _ := core.NewDB(fs.Path, fs, time, core.DefaultConfig)

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

	db, _ := core.NewDB(fs.Path, fs, time, core.DefaultConfig)

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

	db, _ := core.NewDB("", fs, time, core.DefaultConfig)

	key := []byte("foo")
	val := []byte("bar")

	_ = db.Put(key, val)

	err := db.Delete(key)

	assert.NoError(t, err)

	_, err = db.Get(key)

	assert.ErrorIs(t, err, core.ErrKeyNotFound)
}

func TestShould_Not_Be_Able_To_Retrieve_Deleted_Key_After_Startup(t *testing.T) {
	fs := caskfs.NewInMemory()

	var time testutil.Time

	db, _ := core.NewDB("", fs, time, core.DefaultConfig)

	key := []byte("foo")
	val := []byte("bar")

	_ = db.Put(key, val)
	_ = db.Delete(key)

	db, _ = core.NewDB("", fs, time, core.DefaultConfig)

	_, err := db.Get(key)

	assert.ErrorIs(t, err, core.ErrKeyNotFound)
}

func TestShould_Be_Able_To_Reset_Deleted_Key(t *testing.T) {
	fs := caskfs.NewInMemory()

	var time testutil.Time

	db, _ := core.NewDB("", fs, time, core.DefaultConfig)

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

	db, _ := core.NewDB("", fs, time, core.DefaultConfig)

	err := db.Delete([]byte("i-dont-exist"))

	assert.ErrorIs(t, err, core.ErrKeyNotFound)
}

func TestShould_Fetch_All_Keys_In_Order(t *testing.T) {
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

	db, _ := core.NewDB(fs.Path, fs, time, core.DefaultConfig)

	wantKeys := []string{"foo", "bar", "foobar", "baz"}
	gotKeys := db.Keys()

	sort.Strings(gotKeys)
	sort.Strings(wantKeys)

	assert.Equal(t, wantKeys, gotKeys)
}

func TestShould_Not_Fetch_Removed_Keys(t *testing.T) {
	fs := caskfs.NewInMemory()

	var time testutil.Time

	db, _ := core.NewDB("", fs, time, core.DefaultConfig)

	db.Put([]byte("foo"), []byte("val"))
	db.Put([]byte("baz"), []byte("val"))
	db.Put([]byte("bar"), []byte("val"))
	db.Delete([]byte("baz"))

	wantKeys := []string{"foo", "bar"}
	gotKeys := db.Keys()

	sort.Strings(gotKeys)
	sort.Strings(wantKeys)

	assert.Equal(t, wantKeys, gotKeys)
}

func TestShould_Return_Empty_Keys_Slice_For_Empty_DB(t *testing.T) {
	var time testutil.Time

	db, _ := core.NewDB("", caskfs.NewInMemory(), time, core.DefaultConfig)

	assert.Equal(t, []string{}, db.Keys())
}

func TestRotate_Data_Files_When_Threshold_Size_Is_Exceeded(t *testing.T) {
	currDataFileSizeB := int64(65530)

	fs := testutil.NewFS().WithToppedUpDataFile(currDataFileSizeB)

	var time testutil.Time

	db, _ := core.NewDB(fs.Path, fs, time, core.Config{
		MaxDataFileSize: 65546,
	})

	err := db.Put([]byte("foo"), aValue(7, 'a'))

	assert.NoError(t, err)

	fs.VerifyDataFileIsRotated(t)
	fs.VerifyWriteGoesToNewlyActiveDataFile(t)
}

func TestShould_Fetch_Value_From_Rotated_File(t *testing.T) {
	dbName := fmt.Sprintf("gocask_db_%d", gotime.Now().Unix())
	dbPath := gopath.Join(os.TempDir(), dbName)

	defer os.RemoveAll(dbPath)

	var time testutil.Time

	db, _ := core.NewDB(dbPath, caskfs.NewDisk(core.GoTime{}), time, core.Config{
		MaxDataFileSize: 20,
	})

	keyOld := []byte("akey")
	valOld := aValue(15, 'd')

	err := db.Put(keyOld, valOld)

	assert.NoError(t, err)

	keyNew := []byte("anotherkey")
	valNew := aValue(10, 'b')

	err = db.Put(keyNew, valNew)

	assert.NoError(t, err)

	gotOld, err := db.Get(keyOld)

	assert.NoError(t, err)
	assert.Equal(t, valOld, gotOld)

	gotNew, err := db.Get(keyNew)

	assert.NoError(t, err)
	assert.Equal(t, valNew, gotNew)
}

func aValue(n int, b byte) []byte {
	buf := make([]byte, n)

	for i := 0; i < n; i++ {
		buf[i] = b
	}

	return buf
}

func TestPut_Should_Report_Failed_Write_Error(t *testing.T) {
	wantErr := errors.New("an error")

	fs := testutil.NewFS().WithFailWithErrOnWrite(wantErr)

	var time testutil.Time

	db, _ := core.NewDB(fs.Path, fs, time, core.DefaultConfig)

	err := db.Put([]byte("key"), []byte("val"))

	assert.ErrorIs(t, err, err)
}

func TestPut_Should_Report_Typed_Key_Not_Found_Error(t *testing.T) {
	fs := testutil.NewFS().WithMockWriteSupport()

	var time testutil.Time

	db, _ := core.NewDB(fs.Path, fs, time, core.DefaultConfig)

	_, err := db.Get([]byte("foo"))

	assert.ErrorIs(t, err, core.ErrKeyNotFound)
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

	db, _ := core.NewDB(fs.Path, fs, time, core.DefaultConfig)

	_, err := db.Get(key)

	assert.ErrorIs(t, err, wantErr)
}

func TestShould_Tolerate_Partial_Write_On_Put(t *testing.T) {
	var time testutil.Time

	key := []byte("key")
	val := []byte("foobarbaz")

	path := "mydb"

	fs := testutil.NewInMemory(caskfs.NewInMemory()).
		WithPartialWriteFor(key)

	db, _ := core.NewDB(path, fs, time, core.DefaultConfig)

	err := db.Put([]byte("user"), []byte("user123456"))

	assert.NoError(t, err)

	err = db.Put(key, val)

	assert.ErrorIs(t, err, core.ErrPartialWrite)

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

	db, err := core.NewDB(path, inMem, time, core.DefaultConfig)

	assert.NoError(t, err)

	err = db.Put(key, val)

	assert.NoError(t, err)

	db, _ = core.NewDB(path, testutil.NewInMemory(inMem).
		WithPartialWriteFor(key), time, core.DefaultConfig)

	err = db.Delete(key)

	assert.ErrorIs(t, err, core.ErrPartialWrite)

	wantKey := []byte("ishould")
	wantVal := []byte("befine")

	err = db.Put(wantKey, wantVal)

	assert.NoError(t, err)

	gotVal, err := db.Get(wantKey)

	assert.NoError(t, err)
	assert.Equal(t, wantVal, gotVal)
}

func TestShould_Validate_Empty_Keys(t *testing.T) {
	validateKey(t, []byte{})
}

func TestShould_Validate_Nil_Keys(t *testing.T) {
	validateKey(t, nil)
}

func validateKey(t *testing.T, key []byte) {
	db := getInMemDB(t)

	defer db.Close()

	_, err := db.Get(key)

	assert.ErrorIs(t, err, core.ErrInvalidKey)

	err = db.Put(key, []byte("foo"))

	assert.ErrorIs(t, err, core.ErrInvalidKey)

	err = db.Delete(key)

	assert.ErrorIs(t, err, core.ErrInvalidKey)
}

func TestShould_Validate_Nil_Value(t *testing.T) {
	db := getInMemDB(t)

	defer db.Close()

	err := db.Put([]byte("foo"), nil)

	assert.ErrorIs(t, err, core.ErrInvalidValue)
}

func getInMemDB(t *testing.T) *core.DB {
	var time testutil.Time

	path := "mydb"

	inMem := caskfs.NewInMemory()

	db, err := core.NewDB(path, inMem, time, core.DefaultConfig)

	assert.NoError(t, err)

	return db
}

func TestShould_Fail_CRC_Check(t *testing.T) {
	fs := testutil.NewFS().
		WithMockWriteSupport().
		WithMockValue([]byte("corrupted"))

	var time testutil.Time

	db, _ := core.NewDB(fs.Path, fs, time, core.DefaultConfig)

	key := []byte("foo")
	val := []byte("uncorrupted")

	err := db.Put(key, val)

	assert.NoError(t, err)

	got, err := db.Get(key)

	assert.ErrorIs(t, err, core.ErrCRCFailed)
	assert.Nil(t, got)
}
