.PHONY : test
test :
	go test -shuffle=on ./...

.PHONY : test-race
test-race :
	go test -shuffle=on -race -timeout=20m ./...

.PHONY: coverage
coverage:
	go test -count=1 -shuffle=on -covermode=atomic -coverpkg=./... -coverprofile=cover.prof ./...
	go tool cover -func cover.prof | grep -e "^total:"

.PHONY : clean
clean :
	rm -fr ./build/*

.PHONY : lint
lint:
	@./build/bin/golangci-lint run --config ./.golangci.yml

.PHONY : lintci-deps
lintci-deps:
	rm -f ./build/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./build/bin v1.52.1

.PHONY : install-deps
install-deps:
	go get github.com/JekaMas/go-mutesting/cmd/go-mutesting@v1.1.2

.PHONY : mut
mut:
	MUTATION_TEST=on go-mutesting --blacklist=".github/mut_blacklist" --config=".github/mut_config.yml" ./... &> .stats.msi
	@echo MSI: `jq '.stats.msi' report.json`
