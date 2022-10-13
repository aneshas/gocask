package fs_test

import (
	"errors"
	"fmt"
	"github.com/aneshas/gocask/core"
	"github.com/aneshas/gocask/internal/fs"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

func TestDiskFS_Should_Report_Existing_DB_Named_File_As_An_Error(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	file, err := disk.Open("testdata/dbfile")

	assert.Error(t, err)
	assert.Nil(t, file)
}

func TestDiskFS_Should_Create_New_DB(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	db, err := os.MkdirTemp("", "newdb")

	defer os.RemoveAll(db)

	assert.NoError(t, err)

	db = fmt.Sprintf("%s/foodb", db)

	file, err := disk.Open(db)

	assert.NoError(t, err)

	assert.NotNil(t, file)
	assert.DirExists(t, db)
	assert.FileExists(t, path.Join(db, file.Name()+".csk"))
}

func TestDiskFS_Should_Rotate_Active_Data_File_DB(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	db, _ := os.MkdirTemp("", "newdb")

	file, _ := disk.Open(db)

	defer os.RemoveAll(db)

	newFile, err := disk.Rotate(db)

	assert.NoError(t, err)

	oldData := file.Name() + ".csk"
	newData := newFile.Name() + ".csk"

	assert.NotEqual(t, newData, oldData)
	assert.FileExists(t, path.Join(db, oldData))
	assert.FileExists(t, path.Join(db, newData))
}

func TestDiskFS_Should_Open_Latest_Data_File_For_Existing_DB(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	file, err := disk.Open("testdata/defaultdb")

	assert.NoError(t, err)
	assert.Equal(t, "data_1_12", file.Name())
}

func TestDiskFS_Should_Walk_Cask_Data_Files(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	var files []string

	err := disk.Walk("testdata/largedb", func(file core.File) error {
		files = append(files, file.Name())

		return nil
	})

	wantFiles := []string{"data_0_1663009510", "data_1_1663009599", "data_2_1663009610"}

	assert.NoError(t, err)
	assert.Equal(t, wantFiles, files)
}

func TestDiskFS_Walk_Reports_WalkFn_Error(t *testing.T) {
	wantErr := errors.New("an error")
	disk := fs.NewDisk(core.GoTime{})

	err := disk.Walk("testdata/largedb", func(file core.File) error {
		return wantErr
	})

	assert.ErrorIs(t, err, wantErr)
}

func TestDiskFS_Walk_Should_Use_Hint_Files(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	var files []string

	err := disk.Walk("testdata/hintsdb", func(file core.File) error {
		files = append(files, file.Name())

		return nil
	})

	wantFiles := []string{
		"data_0_1664540335.a",
		"data_1_1664540335.a",
		"data_2_1664540335.a",
		"data_3_1664540335.a",
		"data_4_1664540335",
	}

	assert.NoError(t, err)
	assert.Equal(t, wantFiles, files)
}

func TestDiskFS_Walk_Should_Ignore_Tmp_Files(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	var files []string

	err := disk.Walk("testdata/tmpdb", func(file core.File) error {
		files = append(files, file.Name())

		return nil
	})

	wantFiles := []string{
		"data_0_1664540335",
		"data_1_1664540335",
		"data_2_1664540335.a",
		"data_3_1664540335.a",
		"data_4_1664540335",
	}

	assert.NoError(t, err)
	assert.Equal(t, wantFiles, files)
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

	disk := fs.NewDisk(core.GoTime{})

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
	disk := fs.NewDisk(core.GoTime{})

	count := 10
	data := make([]byte, count)

	_, err := disk.ReadFileAt("testdata/readdb", "data-0001", data, 100)

	assert.Error(t, err)
}

func TestShould_Read_File_Size(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	f, _ := disk.Open("./testdata/sizedb")

	assert.Equal(t, int64(13), f.Size())
}

func TestFile_Write_Should_Should_Update_File_Size(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	db, _ := os.MkdirTemp("", "db0003")

	defer os.RemoveAll(db)

	f, _ := disk.Open(db)

	assert.Equal(t, int64(0), f.Size())

	data := []byte("foobar")

	f.Write(data)

	assert.Equal(t, int64(len(data)), f.Size())
}

func TestReadFileAt_Should_Report_Nonexistent_File_Error(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	_, err := disk.ReadFileAt("i-do-not", "exist", nil, 0)

	assert.Error(t, err)
}

func TestDiskFS_Should_Truncate_And_Open_Existing_File(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	db, _ := os.MkdirTemp("", "newdb")

	defer os.RemoveAll(db)

	file := "some_file"
	fpath := path.Join(db, fmt.Sprintf("%s.csk", file))

	err := os.WriteFile(fpath, []byte("abcdefg"), 0755)

	assert.NoError(t, err)

	f, err := disk.OTruncate(db, file)

	assert.NoError(t, err)

	assert.Equal(t, file, f.Name())
	assert.Equal(t, int64(0), f.Size())

	assert.FileExists(t, fpath)
}

func TestDiskFS_Should_Move_File(t *testing.T) {
	disk := fs.NewDisk(core.GoTime{})

	db, _ := os.MkdirTemp("", "newdb")

	defer os.RemoveAll(db)

	file := "some_file"
	newFile := "some_file_moved"

	_, err := disk.OTruncate(db, file)

	assert.NoError(t, err)

	err = disk.Move(db, file, newFile)

	assert.NoError(t, err)

	assert.NoFileExists(t, path.Join(db, fmt.Sprintf("%s.csk", file)))
	assert.FileExists(t, path.Join(db, fmt.Sprintf("%s.csk", newFile)))
}
