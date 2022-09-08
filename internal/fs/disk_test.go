package fs_test

import (
	"errors"
	"fmt"
	"github.com/aneshas/gocask/internal/fs"
	"github.com/aneshas/gocask/pkg/cask"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

func TestDiskFS_Should_Report_Existing_DB_Named_File_As_An_Error(t *testing.T) {
	disk := fs.NewDisk()

	file, err := disk.Open("testdata/dbfile")

	assert.Error(t, err)
	assert.Nil(t, file)
}

func TestDiskFS_Should_Create_New_DB(t *testing.T) {
	disk := fs.NewDisk()

	dbName := fmt.Sprintf("gocask_db_%d", time.Now().Unix())
	db := path.Join(os.TempDir(), dbName)

	file, err := disk.Open(db)

	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.DirExists(t, db)
	assert.FileExists(t, path.Join(db, "data.csk"))
}

func TestDiskFS_Should_Open_Existing_DB(t *testing.T) {
	disk := fs.NewDisk()

	file, err := disk.Open("testdata/defaultdb")

	assert.NoError(t, err)
	assert.NotNil(t, file)
}

func TestDiskFS_Should_Walk_Cask_Data_Files(t *testing.T) {
	disk := fs.NewDisk()

	var files []string

	err := disk.Walk("testdata/largedb", func(file cask.File) error {
		files = append(files, file.Name())

		return nil
	})

	wantFiles := []string{"data", "data01", "data02"}

	assert.NoError(t, err)
	assert.Equal(t, wantFiles, files)
}

func TestDiskFS_Walk_Reports_WalkFn_Error(t *testing.T) {
	wantErr := errors.New("an error")
	disk := fs.NewDisk()

	err := disk.Walk("testdata/largedb", func(file cask.File) error {
		return wantErr
	})

	assert.ErrorIs(t, err, wantErr)
}

func TestDiskFS_Should_Read_File_Value_At_Offset(t *testing.T) {
	cases := []struct {
		val    string
		offset int64
	}{
		{
			val:    "somevalue0",
			offset: 0,
		},
		{
			val:    "anothervalue",
			offset: 10,
		},
		{
			val:    "somethingelse",
			offset: 22,
		},
	}

	disk := fs.NewDisk()

	for _, tc := range cases {
		t.Run(fmt.Sprintf("read %s", tc.val), func(t *testing.T) {
			count := len(tc.val)
			data := make([]byte, count)

			n, err := disk.ReadFileAt("testdata/readdb", "data-0002", data, tc.offset)

			assert.NoError(t, err)
			assert.Equal(t, count, n)
			assert.Equal(t, []byte(tc.val), data)
		})
	}
}

func TestDiskFS_Should_Report_Out_Of_Bounds_Read(t *testing.T) {
	disk := fs.NewDisk()

	count := 10
	data := make([]byte, count)

	_, err := disk.ReadFileAt("testdata/readdb", "data-0001", data, 100)

	assert.Error(t, err)
}

// TODO - Test Order when we know it
