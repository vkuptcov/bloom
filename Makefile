GOLANG_CI = $(shell which golangci-lint)
GO = go

lint:
	$(GOLANG_CI) run -v --timeout 30m

test:
	@$(GO) clean -testcache
	$(GO) test -mod=mod -parallel 4 ./... -coverprofile coverage.out


test-coverage-report: test
	echo "Test coverage:" && $(GO) tool cover -func coverage.out