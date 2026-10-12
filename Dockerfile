# syntax=docker/dockerfile:1.6
FROM golang:1.24-alpine AS builder
WORKDIR /src/inflora-ws-gateway
COPY inflora-shared /src/inflora-shared
COPY inflora-ws-gateway/go.mod inflora-ws-gateway/go.sum ./
RUN go mod download
COPY inflora-ws-gateway .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/inflora-ws-gateway ./cmd/gateway

FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget && addgroup -S inflora && adduser -S -G inflora inflora
COPY --from=builder /out/inflora-ws-gateway /usr/local/bin/inflora-ws-gateway
USER inflora:inflora
ENTRYPOINT ["/usr/local/bin/inflora-ws-gateway"]
EXPOSE 8083
