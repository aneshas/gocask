package gocask_test

import (
	"bufio"
	"fmt"
	"github.com/aneshas/gocask"
	"github.com/aneshas/gocask/core"
	"github.com/aneshas/gocask/core/testutil"
	"github.com/aneshas/gocask/internal/fs"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"
)

func TestInMemory_DB_Should_Store_And_Retrieve_A_Set_Of_Key_Val_Pairs(t *testing.T) {
	db, _ := gocask.Open(core.InMemoryDB)
	defer db.Close()

	writeReadAndAssert(t, db)
}

func TestDisk_DB_Should_Store_And_Retrieve_A_Set_Of_Key_Val_Pairs(t *testing.T) {
	dbName := fmt.Sprintf("cask_db_%d", time.Now().Unix())
	dbPath := path.Join(os.TempDir(), dbName)

	defer os.RemoveAll(dbPath)

	db, _ := gocask.Open(
		dbName,
		gocask.WithDataDir(os.TempDir()),
		gocask.WithMaxDataFileSize(10*gocask.GB),
	)
	defer db.Close()

	writeReadAndAssert(t, db)
}

func writeReadAndAssert(t *testing.T, db *core.DB) {
	file, err := os.Open("testdata/data.txt")

	assert.NoError(t, err)

	defer file.Close()

	scanner := bufio.NewScanner(file)

	entries := make(map[string][]byte)

	for scanner.Scan() {
		text := scanner.Text()

		parts := strings.Split(text, "|")

		val := []byte(parts[1])

		entries[parts[0]] = val

		err := db.Put([]byte(parts[0]), val)

		assert.NoError(t, err)
	}

	assert.NoError(t, err)

	for key, want := range entries {
		t.Run(fmt.Sprintf("get_%s", key), func(t *testing.T) {
			got, err := db.Get([]byte(key))

			assert.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func BenchmarkDisk_Put_1(b *testing.B) {
	benchDiskPut(b, 1)
}

func BenchmarkDisk_Put_100(b *testing.B) {
	benchDiskPut(b, 100)
}

func BenchmarkDisk_Put_1000(b *testing.B) {
	benchDiskPut(b, 1000)
}

func BenchmarkDisk_Put_100000(b *testing.B) {
	benchDiskPut(b, 100000)
}

func BenchmarkDisk_Put_500000(b *testing.B) {
	benchDiskPut(b, 500000)
}

func benchDiskPut(b *testing.B, n int) {
	dbName := fmt.Sprintf("gocask_db_%d", time.Now().Unix())
	dbPath := path.Join(os.TempDir(), dbName)

	defer os.RemoveAll(dbPath)

	db, err := gocask.Open(dbPath)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			// TODO Larger value
			err := db.Put([]byte("user:123456"), []byte("lorem ipsum sit dolor amet - lorem ipsum sit dolor amet"))
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestGiven_No_Deleted_Entries_Should_Generate_Only_Hints(t *testing.T) {
	dbPath := os.TempDir()
	dbName := "mergedb00"

	db := generateMergeDBs(t, 30, dbPath, dbName)

	defer db.Close()

	// TODO Testcases only change db name
	err := db.Merge()

	assert.NoError(t, err)

	if *genGolden {
		t.Skip()
	}

	defer os.RemoveAll(path.Join(dbPath, dbName))

	assertEqualDBs(t, dbPath, dbName)

	// TODO compare folders ?
	// multiple cases eg:
	// single file no merge only hint
	// single file with some deleted
	// single file all deleted
	// multiple files

	// test temp files somehow or no?

	// Then
	// use the same data and test startup
	// use the same data and test startup (with deleted entries)
}

func TestGiven_No_Deleted_Entries_Should_Generate_All_Hints(t *testing.T) {
	dbPath := os.TempDir()
	dbName := "mergedb01"

	db := generateMergeDBs(t, 30, dbPath, dbName)

	defer db.Close()

	err := db.Merge()
	assert.NoError(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	if *genGolden {
		t.Skip()
	}

	defer os.RemoveAll(path.Join(dbPath, dbName))

	assertEqualDBs(t, dbPath, dbName)
}

func TestShould_Cleanup_Deleted_Entries(t *testing.T) {
	dbPath := os.TempDir()
	dbName := "mergedb02"

	db := generateMergeDBs(t, 30, dbPath, dbName)

	deleteKeys(t, db, "foo", "john")

	defer db.Close()

	err := db.Merge()
	assert.NoError(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	if *genGolden {
		t.Skip()
	}

	defer os.RemoveAll(path.Join(dbPath, dbName))

	assertEqualDBs(t, dbPath, dbName)
}

func TestShould_Skip_Merged_Data_File(t *testing.T) {
	dbPath := os.TempDir()
	dbName := "mergedb03"

	db := generateMergeDBs(t, 110, dbPath, dbName)

	defer db.Close()

	err := db.Merge()
	assert.NoError(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	if *genGolden {
		t.Skip()
	}

	defer os.RemoveAll(path.Join(dbPath, dbName))

	assertEqualDBs(t, dbPath, dbName)
}

func TestShould_Startup_From_Zero_Length_Data_And_Hint_Files(t *testing.T) {
	db, err := gocask.Open("mergedb02", gocask.WithDataDir("./testdata"))

	assert.NoError(t, err)

	defer db.Close()

	for _, tc := range mergeSeed {

		val, err := db.Get([]byte(tc.key))

		if tc.key == "foo" || tc.key == "john" {
			assert.ErrorIs(t, err, core.ErrKeyNotFound)

			continue
		}

		assert.NoError(t, err)
		assert.Equal(t, val, []byte(tc.val))
	}
}

func TestShould_Startup_From_Hint_File(t *testing.T) {
	db, err := gocask.Open("mergedb01", gocask.WithDataDir("./testdata"))

	assert.NoError(t, err)

	defer db.Close()

	for _, tc := range mergeSeed {
		val, err := db.Get([]byte(tc.key))

		assert.NoError(t, err)
		assert.Equal(t, val, []byte(tc.val))
	}
}

func assertEqualDBs(t *testing.T, dbPath, dbName string) {
	goldenPath := path.Join("./testdata", dbName)
	gotPath := path.Join(dbPath, dbName)

	err := exec.Command("diff", "-r", goldenPath, gotPath).Run()

	assert.NoError(t, err)
}

func deleteKeys(t *testing.T, db *core.DB, keys ...string) {
	for _, k := range keys {
		err := db.Delete([]byte(k))

		assert.NoError(t, err)
	}
}

func generateMergeDBs(t *testing.T, size int64, dbPath, dbName string) *core.DB {
	if *genGolden {
		dbPath = "./testdata"
	}

	_ = os.RemoveAll(path.Join(dbPath, dbName))

	var tt testutil.Time

	db, err := core.NewDB(dbName, fs.NewDisk(tt), tt, core.Config{
		MaxDataFileSize: size,
		DataDir:         dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, s := range mergeSeed {
		if s.del {

			continue
		}

		err = db.Put([]byte(s.key), []byte(s.val))
		if err != nil {
			t.Fatal(err)
		}
	}

	return db
}
