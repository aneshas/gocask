package fs_test

import (
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

// TODO - Test Order when we know it

func TestDiskFS_Should_Read_File_Value_At_Zero_Offset(t *testing.T) {
	// TODO Table test

	disk := fs.NewDisk()

	count := 10
	data := make([]byte, count)

	n, err := disk.ReadFileAt("testdata/readdb", "data-0001", data, 0)

	assert.NoError(t, err)
	assert.Equal(t, count, n)
}
