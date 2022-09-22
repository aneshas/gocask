package fs_test

import (
	"fmt"
	"github.com/aneshas/gocask/core"
	"github.com/aneshas/gocask/internal/fs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShould_Read_Written_Value(t *testing.T) {
	mem := fs.NewInMemory()

	f, _ := mem.Open("")

	b := []byte("foobarbaz")

	n, err := f.Write(b)

	assert.NoError(t, err)
	assert.Equal(t, len(b), n)

	gotB := make([]byte, len(b))

	n, err = mem.ReadFileAt("", "", gotB, 0)

	assert.NoError(t, err)
	assert.Equal(t, len(b), n)
	assert.Equal(t, b, gotB)

	assert.NoError(t, f.Close())
}

func TestSize_Returns_0(t *testing.T) {
	mem := fs.NewInMemory()

	f, _ := mem.Open("")

	assert.Equal(t, int64(0), f.Size())
}

func TestRotation_Is_A_NoOp(t *testing.T) {
	mem := fs.NewInMemory()

	f, _ := mem.Open("")

	newF, _ := mem.Rotate("")

	assert.Equal(t, f, newF)
}

func TestWalk_Walks_Over_Single_Data_File(t *testing.T) {
	mem := fs.NewInMemory()

	f, _ := mem.Open("")

	b := []byte("foobarbaz")

	_, _ = f.Write(b)

	err := mem.Walk("", func(file core.File) error {
		gotB := make([]byte, len(b))

		n, err := file.Read(gotB)

		assert.NoError(t, err)
		assert.Equal(t, len(b), n)
		assert.Equal(t, b, gotB)

		return nil
	})

	assert.NoError(t, err)
}

func TestShould_Return_DataFile_Name(t *testing.T) {
	mem := fs.NewInMemory()

	f, _ := mem.Open("")

	assert.Equal(t, "data", f.Name())
}

func TestWalk_Should_Propagate_Error(t *testing.T) {
	mem := fs.NewInMemory()
	mem.Open("")

	e := fmt.Errorf("an error")

	err := mem.Walk("", func(_ core.File) error {
		return e
	})

	assert.ErrorIs(t, e, err)
}