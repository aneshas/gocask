//go:build tools
// +build tools

package tools

import (
	_ "github.com/twitchtv/twirp/protoc-gen-twirp"
	_ "github.com/vektra/mockery/v2/cmd"
)
