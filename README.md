# GoCask
[![Go Test](https://github.com/aneshas/gocask/actions/workflows/test.yml/badge.svg)](https://github.com/aneshas/gocask/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/aneshas/gocask/badge.svg?branch=trunk)](https://coveralls.io/github/aneshas/gocask?branch=trunk)
[![Go Report Card](https://goreportcard.com/badge/github.com/aneshas/gocask)](https://goreportcard.com/report/github.com/aneshas/gocask)
[![Go Reference](https://pkg.go.dev/badge/github.com/aneshas/gocask.svg)](https://pkg.go.dev/github.com/aneshas/gocask)

Go implementation of Bitcask - A Log-Structured Hash Table for Fast Key / Value Data as defined per [this](https://riak.com/assets/bitcask-intro.pdf) paper and with help from [this](https://github.com/avinassh/py-caskdb) repo.

A learning venture into database development.
Special thanks go to the amazing [Ben Johnson](https://medium.com/@benbjohnson) for pointing me in the right direction and being as helpful as he was.

# Features (as defined by the paper)
- Low latency per item read or written
- High throughput, especially when writing an incoming stream of random items
- Ability to handle datasets much larger than RAM w/o degradation
- Crash friendliness, both in terms of fast recovery and not losing data
- Ease of backup and restore
- A relatively simple, understandable (and thus supportable) code structure and data format
- Predictable behavior under heavy access load or large volume
- Data files are rotated based on the user defined data file size (10GB default)
- A license that allowed for easy use

# How to Use/Run
There are two ways to use gocask

## Using gocask as a library (embedded db) in your own app 
`go get github.com/aneshas/gocask/cmd/gocask` and use the api. See the [docs](https://pkg.go.dev/github.com/aneshas/gocask#readme-gocask) 

## Running as a standalone process
If you have go installed:
- `go install github.com/aneshas/gocask/cmd/gocask@latest`
- `go install github.com/aneshas/gocask/cmd/gccli@latest`

### Run db server
Then run `gocask` which will run the db engine itself, open `default` db and start grpc (twirp) server on `localhost:8888` (Run `gocask -help` to see config options and the defaults)

### Interact with server via cli
While the server is running you can interact with it via `gccli` binary:
- `gccli keys` - list stored keys
- `gccli put somekey someval` - stores the key value pair
- `gccli get somekey` - retrieves the value stored under the key
- `gccli del somekey` - deletes the value stored under the key

`gccli` is just meant as a simple probing tool, and you can generate your own client you can use the .proto definition included (or use the pre generated [go client](./rpc).
 
If you don't have go installed, you can go to [releases](https://github.com/aneshas/gocask/releases) download latest release and go through the same process as above.


# Still to come
Since the primary motivation for this repo was learning more about how db engines work and although it could already be used, it's far from production ready. With that being said, I do plan to maintain and extend it in the future.

Some things that are on my mind:
- Current key deletion is a soft delete (implement garbage collection of deleted keys)
- Double down on tests (maybe fuzzing)
- Add benchmarks
- Use hint file to improve the startup time
- Support for multiple processes and locking
- Making it distributed 
- An [eventstore](https://github.com/aneshas/eventstore) spin off (use gocask instead of sqlite)
