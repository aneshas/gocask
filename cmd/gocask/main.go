package main

import (
	"context"
	"fmt"
	"github.com/aneshas/flags"
	"github.com/aneshas/flags/env"
	"github.com/aneshas/gocask"
	"github.com/aneshas/gocask/core"
	"github.com/aneshas/gocask/rpc"
	"log"
	"net/http"
	"os"
)

func main() {
	var fs flags.FlagSet

	var (
		dataDir = fs.String("datadir", "Directory where databases are stored (default ~/gcdata)", "", env.Named("DATADIR"))
		dbName  = fs.String("db", "DB name to connect to", "default", env.Named("DBNAME"))
		maxSize = fs.Int64("maxsize", "Max data file size in bytes (default 2GB)", 0, env.Named("MAX_DATA_FILE_SIZE"))
		port    = fs.Int("port", "Server port", 8888, env.Named("PORT"))
	)

	fs.Parse(os.Args)

	var opts []gocask.Option

	if *maxSize > 0 {
		opts = append(opts, gocask.WithMaxDataFileSize(*maxSize))
	}

	if *dataDir != "" {
		opts = append(opts, gocask.WithDataDir(*dataDir))
	}

	fmt.Printf("Opening %s database...", *dbName)

	db, err := gocask.Open(*dbName, opts...)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(" Done.")

	twirpServer := rpc.NewGoCaskServer(&server{db})

	fmt.Printf("Started GoCask server on localhost:%d\n", *port)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), twirpServer))
}

type server struct {
	db *core.DB
}

// Put a value
func (g *server) Put(_ context.Context, request *rpc.PutRequest) (*rpc.Empty, error) {
	return &rpc.Empty{}, g.db.Put(request.Key, request.Value)
}

// Get a value
func (g *server) Get(_ context.Context, request *rpc.GetRequest) (*rpc.Entry, error) {
	val, err := g.db.Get(request.Key)
	if err != nil {
		return nil, err
	}

	return &rpc.Entry{
		Key:   request.Key,
		Value: val,
	}, nil
}

// Delete a value
func (g *server) Delete(_ context.Context, request *rpc.DeleteRequest) (*rpc.Empty, error) {
	return &rpc.Empty{}, g.db.Delete(request.Key)
}

// Keys gets all keys
func (g *server) Keys(_ context.Context, _ *rpc.Empty) (*rpc.KeysResponse, error) {
	keys := g.db.Keys()

	return &rpc.KeysResponse{
		Mkeys: keys,
	}, nil
}
