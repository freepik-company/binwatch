FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /app/binwatch cmd/main.go

FROM mysql:8.0
WORKDIR /app
COPY --from=builder /app/binwatch /app/
COPY docs/samples/config-sample.yaml /app/config.yaml
ENTRYPOINT [ "./binwatch", "sync" ]
CMD ["--config", "/app/config.yaml"]