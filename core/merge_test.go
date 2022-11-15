package core_test

import (
	"testing"

	"github.com/aneshas/gocask/core"
	"github.com/aneshas/gocask/core/testutil"
	"github.com/stretchr/testify/assert"
)

func TestShould_Merge_And_Hint_Single_File_With_A_Missing_Value(t *testing.T) {
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
	}

	mergedFile := "data0"

	fs := testutil.
		NewFS().
		WithMergeSetup(mergedFile).
		UseMockDataFiles()

	for _, tc := range seed {
		fs.AddMockDataFileEntry(
			tc.file,
			testutil.Entry(tc.now, []byte(tc.key), []byte(tc.val)),
		)
	}

	var time testutil.Time

	db, _ := core.NewDB(fs.Path, fs, time, core.DefaultConfig)

	db.Delete([]byte(seed[1].key))

	err := db.Merge()

	assert.NoError(t, err)

	me := seed[0]

	fs.VerifyMergeAndHint(
		t,
		mergedFile,
		testutil.Entry(me.now, []byte(me.key), []byte(me.val)),
		testutil.HintEntry(me.now, []byte(me.key), []byte(me.val), 0),
	)
}

// Test open merge fails - error is logged
// Test open hint fails - error is logged
// Test active file is skipped (verify mocks)
// Test hint file is skipped (verify mocks)
// Test corrupted value is skipped (crc)
// Test hint write fails
// Test commit fails (merge move)
// Test commit fails (hint move)
// Bug: keydir holds a reference to tmp merge file (keydir merge)
// v2:
// What to do with zero length files (eg. all keys deleted)

// Threshold related tests:
// - thresholds (do a simple implementation first deploy and iterate)
// - key deleted in future file
// - key updated in future file
// - Merge once, delete, merge again
// - Even merged and hinted files need to be revisited since keys can be deleted later
