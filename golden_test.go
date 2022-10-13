package gocask_test

import "flag"

var mergeSeed = []struct {
	key, val string
	del      bool
}{
	{
		key: "foo",
		val: "foo bar baz",
	},
	{
		key: "john",
		val: "doe foo bar baz",
	},
	{
		key: "jane",
		val: "doe foo bar baz jane",
	},
	{
		key: "max",
		val: "mustermann",
	},
	{
		key: "abc",
		val: "cba",
	},
	{
		key: "def",
		val: "abc foo bar baz",
	},
}

// var genGolden = flag.Bool("golden", true, "regenerate golden files")

var genGolden = flag.Bool("golden", false, "regenerate golden files")
