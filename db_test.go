package gocask_test

import (
	"bufio"
	"fmt"
	"github.com/aneshas/gocask"
	"github.com/aneshas/gocask/core"
	"github.com/stretchr/testify/assert"
	"os"
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

var mergeCases = []struct {
	key, val string
}{
	{
		key: "foo",
		val: "foo bar baz",
	},
	{
		key: "john",
		val: "doe foo bar baz",
	},
	{
		key: "jane",
		val: "doe foo bar baz jane",
	},
	{
		key: "max",
		val: "mustermann",
	},
}

// Merge one file restart, should have all values
// Delete entry, merge one file restart
// Active file not merged

// Thresholds - merge with low threshold, nothing should happen except for hint

func TestMerge(t *testing.T) {
	os.RemoveAll("./testdata/foodb")

	db, _ := gocask.Open(
		"foodb",
		gocask.WithDataDir("./testdata"),
		gocask.WithMaxDataFileSize(1024),
	)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)

		_ = db.Put([]byte(key), []byte(val))

		if i == 50 {
			_ = db.Delete([]byte(key))
		}
	}

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

	db.Close()

	db, err = gocask.Open(
		"foodb",
		gocask.WithDataDir("./testdata"),
		gocask.WithMaxDataFileSize(1024),
	)

	assert.NoError(t, err)

	defer db.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		want := fmt.Sprintf("val_%d", i)

		val, err := db.Get([]byte(key))

		if i == 50 {
			assert.ErrorIs(t, err, core.ErrKeyNotFound)
		} else {
			assert.Equal(t, []byte(want), val)
		}
	}
}
