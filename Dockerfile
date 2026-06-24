FROM golang:alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/server-bin ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/worker-bin ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/mcp-bin ./cmd/mcp

FROM alpine:latest AS server
WORKDIR /app
COPY --from=builder /app/server-bin /app/server-bin
COPY --from=builder /app/migrations /app/migrations
EXPOSE 8080
CMD ["/app/server-bin"]

FROM alpine:latest AS worker
WORKDIR /app
COPY --from=builder /app/worker-bin /app/worker-bin
COPY --from=builder /app/migrations /app/migrations
CMD ["/app/worker-bin"]

FROM alpine:latest AS mcp
WORKDIR /app
COPY --from=builder /app/mcp-bin /app/mcp-bin
EXPOSE 8090
CMD ["/app/mcp-bin"]
