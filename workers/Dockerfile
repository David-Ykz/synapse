FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o agent_worker ./workers/agent_worker.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/agent_worker .
RUN mkdir /data
CMD ["./agent_worker"]