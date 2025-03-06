FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /app/binwatch cmd/main.go

FROM alpine:3.18
RUN apk --no-cache add ca-certificates bash
WORKDIR /app
COPY --from=builder /app/binwatch /app/
COPY docs/samples/config-sample.yaml /app/config.yaml
ENTRYPOINT [ "./binwatch", "watch" ]
CMD ["--config", "/app/config.yaml"]