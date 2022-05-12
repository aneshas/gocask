package gocask_test

import (
	"reflect"
	"testing"

	"github.com/aneshas/gocask"
)

// func TestXxx(t *testing.T) {
// 	db, err := gocask.Open("store")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	defer db.Close()

// 	key := "some-key"
// 	value := []byte("some value")

// 	db.Put("foo", []byte("bar"))
// 	db.Put("foo", []byte("barsdfd"))
// 	db.Put(key, value)
// 	db.Put("bar", []byte("asdfdsbar"))
// 	db.Put("foodd", []byte("bardfd"))

// 	val, err := db.Get(key)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	if !reflect.DeepEqual(value, val) {
// 		t.Fatalf("values not equal")
// 	}
// }

func TestShould_Get_Values(t *testing.T) {
	cases := map[string]string{
		"foo":      "barsdfd",
		"bar":      "asdfdsbar",
		"foodd":    "bardfd",
		"some-key": "some value",
	}

	db, err := gocask.Open("testdata/store")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	for key, value := range cases {
		t.Run(key, func(t *testing.T) {
			val, err := db.Get(key)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual([]byte(value), val) {
				t.Fatalf("values not equal want: %s, got: %s", value, val)
			}
		})
	}
}
