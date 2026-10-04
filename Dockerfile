# syntax=docker/dockerfile:1.6
FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/inflora-ws-gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/inflora-ws-gateway /usr/local/bin/inflora-ws-gateway
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/inflora-ws-gateway"]
EXPOSE 8083
