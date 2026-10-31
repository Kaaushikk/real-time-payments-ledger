# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG BIN=ledger
RUN CGO_ENABLED=0 go build -o /out/service ./cmd/${BIN}

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/service /app/service
USER 65532:65532
ENTRYPOINT ["/app/service"]
