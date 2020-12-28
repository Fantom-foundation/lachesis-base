.PHONY : test
test :
	go test ./...

.PHONY: coverage
coverage:
	go test -coverpkg=./... -coverprofile=cover.prof ./...
	go tool cover -func cover.prof | grep -e "^total:"

.PHONY : clean
clean :
	rm -fr ./build/*
