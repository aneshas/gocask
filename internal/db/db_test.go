package db_test

import (
	"bufio"
	"fmt"
	"github.com/aneshas/gocask/core"
	"github.com/aneshas/gocask/core/testutil"
	"github.com/aneshas/gocask/internal/db"
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
	db, _ := db.Open(core.InMemoryDB)
	defer db.Close()

	writeReadAndAssert(t, db)
}

func TestDisk_DB_Should_Store_And_Retrieve_A_Set_Of_Key_Val_Pairs(t *testing.T) {
	dbName := fmt.Sprintf("cask_db_%d", time.Now().Unix())
	dbPath := path.Join(os.TempDir(), dbName)

	defer os.RemoveAll(dbPath)

	db, _ := db.Open(
		dbName,
		db.WithDataDir(os.TempDir()),
		db.WithMaxDataFileSize(10*db.GB),
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

	db, err := db.Open(dbPath)
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

func TestShould_Startup_From_Zero_Length_Data_And_Hint_Files(t *testing.T) {
	db, err := db.Open("mergedb02", db.WithDataDir("./testdata"))

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
	db, err := db.Open("mergedb01", db.WithDataDir("./testdata"))

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
