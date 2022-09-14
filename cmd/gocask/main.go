package main

import (
	"context"
	"fmt"
	"github.com/aneshas/gocask"
	"github.com/aneshas/gocask/rpc"
	"github.com/labstack/echo/v4"
	"log"
)

func main() {
	e := echo.New()

	db, err := gocask.Open("/foodb")
	if err != nil {
		log.Fatal(err)
	}

	twirpServer := rpc.NewGoCaskServer(&server{db})

	e.POST(fmt.Sprintf("%s*", twirpServer.PathPrefix()), func(c echo.Context) error {
		twirpServer.ServeHTTP(c.Response(), c.Request())
		return nil
	})

	log.Fatal(e.Start(":8888"))
}

type server struct {
	db *gocask.DB
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
