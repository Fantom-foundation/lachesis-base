.PHONY : test
test :
	go test ./...

.PHONY : clean
clean :
	rm -fr ./build/*
