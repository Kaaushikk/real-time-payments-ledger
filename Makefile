.PHONY: test tidy run compose proto

PROTO_OUT ?= .

tidy:
	go mod tidy

proto:
	go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate --output "$(PROTO_OUT)"

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
