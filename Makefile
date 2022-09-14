.PHONY: test
test:
	@go test -count=1 -race -coverprofile=profile.cov -v $(shell go list ./... | grep -vE 'cmd|mocks|testdata|testutil')
	@go tool cover -func=profile.cov | grep total

.PHONY: tools
tools:
	@echo "Downloading and installing proto generators..."
	go install github.com/twitchtv/twirp/protoc-gen-twirp
	go install google.golang.org/protobuf/cmd/protoc-gen-go
	@echo "Done."

.PHONY: proto
proto:
	@echo "Generating go-twirp servers and clients..."
	@protoc \
		--go_out=paths=source_relative:. \
		--twirp_out=paths=source_relative:. \
		rpc/gocask.proto
