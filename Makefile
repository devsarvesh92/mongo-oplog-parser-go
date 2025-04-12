test: 
	go test ./... 2>&1 | grep -v "\[no test files\]"

format:
	go fmt ./...