.PHONY: test tidy run compose proto

tidy:
	go mod tidy

test:
	go test ./...

vet:
	go vet ./...

compose:
	docker compose up --build

loadtest:
	FROM_ACCOUNT=$(FROM) TO_ACCOUNT=$(TO) go run ./cmd/loadtest

chaos:
	go run ./cmd/chaos
