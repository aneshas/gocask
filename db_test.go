package gocask_test

import (
	"bufio"
	"fmt"
	"github.com/aneshas/gocask"
	"github.com/aneshas/gocask/pkg/cask"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

func TestInMemory_DB_Should_Store_And_Retrieve_A_Set_Of_Key_Val_Pairs(t *testing.T) {
	db, _ := gocask.Open(cask.InMemoryDB)
	defer db.Close()

	writeReadAndAssert(t, db)
}

func TestDisk_DB_Should_Store_And_Retrieve_A_Set_Of_Key_Val_Pairs(t *testing.T) {
	dbName := fmt.Sprintf("gocask_db_%d", time.Now().Unix())
	dbPath := path.Join(os.TempDir(), dbName)

	db, _ := gocask.Open(dbPath)
	defer db.Close()

	writeReadAndAssert(t, db)

	assert.NoError(t, os.RemoveAll(dbPath))
}

func writeReadAndAssert(t *testing.T, db *gocask.DB) {
	file, err := os.Open("testdata/big_data.txt")
	if err != nil {
		t.Fatal(err)
	}

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

	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	for key, want := range entries {
		got, err := db.Get([]byte(key))

		assert.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

func TestDisk_DB_Should_Fetch_All_Keys(t *testing.T) {
	db, _ := gocask.Open("testdata/mydb")

	defer db.Close()

	assert.Equal(t, []string{"user123"}, db.Keys())
}
