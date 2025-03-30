test: setup
	go test ./...

setup:
	@docker-compose up -d