test:
	@go test -count=1 -race -coverprofile=profile.cov -v $(go list ./... | grep -vE 'generated|mocks|testdata|testutil')
	@go tool cover -func=profile.cov | grep total
	@rm profile.cov
