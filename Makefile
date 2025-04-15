test: 
	go test ./... 2>&1 | grep -v "\[no test files\]"

format:
	go fmt ./...

build:
	go build -o mongo-oplog-parser ./cmd/mongo-oplog-parser/main.go

install:
	go install ./cmd/mongo-oplog-parser