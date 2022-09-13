package main

import (
	"fmt"
	"github.com/aneshas/gocask"
	"log"
	"os"
)

// Cask would be usable as an embedded store (root) or as an executable (this)
// Executable would provide grpc api (maybe later) for now add http and swagger (made for learning)

func main() {
	db, err := gocask.Open("/Users/anes.hasicic/mydb")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	if len(os.Args) < 2 {
		return
	}

	if os.Args[1] == "put" {
		err = db.Put([]byte(os.Args[2]), []byte(os.Args[3]))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Value saved under %s key\n", os.Args[2])
	}

	if os.Args[1] == "get" {
		val, err := db.Get([]byte(os.Args[2]))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%s: %s\n", os.Args[2], val)
	}

	if os.Args[1] == "del" {
		err := db.Delete([]byte(os.Args[2]))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Deleted: %s\n", os.Args[2])
	}

	if os.Args[1] == "keys" {
		for _, k := range db.Keys() {
			fmt.Println(k)
		}
	}
}
