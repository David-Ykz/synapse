FROM golang:alpine AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o autoscaler ./autoscaler/cmd/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/autoscaler .
CMD ["./autoscaler"]
