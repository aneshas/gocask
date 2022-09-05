package cask_test

import (
	"encoding/binary"
	"fmt"
	"github.com/aneshas/gocask/internal/cask"
	"github.com/aneshas/gocask/internal/cask/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

var bo binary.ByteOrder = binary.LittleEndian

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

	for _, tc := range cases {
		t.Run(fmt.Sprintf("put %s", tc.key), func(t *testing.T) {
			fs := testutil.NewFS().WithWriteSupport()

			db, err := cask.NewDB(fs.Path, fs, testutil.Time(tc.now))

			assert.NoError(t, err)

			key := []byte(tc.key)
			val := []byte(tc.val)

			err = db.Put(key, val)

			assert.NoError(t, err)

			fs.VerifyEntryWritten(t, entry(tc.now, key, val))

			assert.NoError(t, db.Close())
		})
	}
}

// TODO Error values should live in the root

// TODO Errors cases
// file open errors
// path errors (probably propagations)
// non matching write (len, err)
// Empty key
// empty val
// same key behavior

// Test put and get together in sequence - this would test keydir updates ?

func TestShould_Perform_Startup(t *testing.T) {

}

// TODO Test error cases

//func TestShould_Successfully_Fetch_Existing_Values(t *testing.T) {
//	cases := []struct {
//		key  string
//		want string
//	}{
//		{
//			key:  "foo",
//			want: "foo bar baz",
//		},
//		{
//			key:  "name",
//			want: "john doe",
//		},
//		{
//			key:  "1234",
//			want: `{"foo": "bar"}`,
//		},
//		{
//			key:  "foo bar baz",
//			want: "test",
//		},
//	}
//
//	for _, tc := range cases {
//		t.Run(fmt.Sprintf("put %s", tc.key), func(t *testing.T) {
//			path := "path/to/db"
//
//			var file mocks.File
//
//			//file.On("Name").Return("data")
//			//file.On("Write", mock.Anything).Return(0, nil)
//			file.On("Close").Return(nil)
//
//			key := []byte(tc.key)
//			want := []byte(tc.want)
//
//			// TODO Extract this to an entry func
//			wantEntry := appendBytes(
//				u32ToB(1234),
//				u32ToB(uint32(len(key))),
//				u32ToB(uint32(len(want))),
//				key,
//				want,
//			)
//
//			var fs mocks.FS
//
//			fs.On("Open", path).Return(&file, nil)
//			fs.On("ReadFileAt", path, mock.Anything, mock.Anything, 0).Return(wantEntry, nil)
//
//			db, err := cask.NewDB(path, &fs, nil)
//
//			assert.NoError(t, err)
//
//			got, err := db.Get(key)
//
//			assert.NoError(t, err)
//
//			assert.Equal(t, want, got)
//
//			err = db.Close()
//
//			assert.NoError(t, err)
//		})
//	}
//}

func entry(now uint32, key, val []byte) []byte {
	return appendBytes(
		u32ToB(now),
		u32ToB(uint32(len(key))),
		u32ToB(uint32(len(val))),
		key,
		val,
	)
}

func u32ToB(i uint32) []byte {
	b := make([]byte, 4)

	bo.PutUint32(b, i)

	return b
}

func appendBytes(chunks ...[]byte) []byte {
	var b []byte

	for _, c := range chunks {
		b = append(b, c...)
	}

	return b
}
