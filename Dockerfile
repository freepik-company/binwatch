FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /app/binwatch cmd/main.go

# Needed mysqldump 8.0.31 image, so do not executes FLUSH TABLES at
# the beggining of the dump which is locking the tables
FROM mysql:8.0.31
WORKDIR /app
COPY --from=builder /app/binwatch /app/
ENTRYPOINT [ "./binwatch", "sync" ]
CMD ["--config", "/app/config.yaml"]