FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o synapse_broker ./broker/cmd/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/synapse_broker .
RUN mkdir /data
CMD ["./synapse_broker"]