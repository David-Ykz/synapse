FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o synapse_broker ./cmd/broker/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/synapse_broker .
RUN mkdir /data
CMD ["./synapse_broker"]