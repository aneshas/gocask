package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aneshas/gocask/pkg/cask"
	"github.com/aneshas/gocask/rpc"
	"log"
	"net/http"
	"os"
)

func main() {
	client := rpc.NewGoCaskProtobufClient("http://localhost:8888", http.DefaultClient)
	ctx := context.Background()

	if len(os.Args) < 2 {
		return
	}

	if os.Args[1] == "put" {
		_, err := client.Put(
			ctx,
			&rpc.PutRequest{
				Key:   []byte(os.Args[2]),
				Value: []byte(os.Args[3]),
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Value saved under %s key\n", os.Args[2])
	}

	if os.Args[1] == "get" {
		entry, err := client.Get(
			ctx,
			&rpc.GetRequest{
				Key: []byte(os.Args[2]),
			},
		)
		if err != nil {
			if errors.Is(err, cask.ErrKeyNotFound) {
				fmt.Println(err)
				return
			}

			log.Fatal(err)
		}

		fmt.Printf("%s: %s\n", os.Args[2], entry.Value)
	}

	if os.Args[1] == "del" {
		_, err := client.Delete(ctx, &rpc.DeleteRequest{
			Key: []byte(os.Args[2]),
		})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Deleted: %s\n", os.Args[2])
	}

	if os.Args[1] == "keys" {
		keys, err := client.Keys(ctx, nil)
		if err != nil {
			log.Fatal(err)
		}

		for _, k := range keys.Mkeys {
			fmt.Println(k)
		}
	}
}
