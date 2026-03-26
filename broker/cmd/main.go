package main

import (
	"log"
	"os"
	"strconv"
	broker "synapse/broker/internal" // Adjust path as needed
)

const (
	BROKER_PORT        = "BROKER_PORT"
	BROKER_FILEPATH    = "BROKER_FILEPATH"
	BROKER_BUFFER_SIZE = "BROKER_BUFFER_SIZE"
)

func main() {
	// 1. Pull config from Environment (Best for K3s)
	portStr, ok := os.LookupEnv(BROKER_PORT)
	if !ok {
		log.Fatalf("Failed to get environment variable %s", BROKER_PORT)
	}
	filePath, ok := os.LookupEnv(BROKER_FILEPATH)
	if !ok {
		log.Fatalf("Failed to get environment variable %s", BROKER_FILEPATH)
	}
	bufferStr, ok := os.LookupEnv(BROKER_BUFFER_SIZE)
	if !ok {
		log.Fatalf("Failed to get environment variable %s", BROKER_BUFFER_SIZE)
	}

	port, _ := strconv.Atoi(portStr)
	bufferSize, _ := strconv.Atoi(bufferStr)

	server := broker.NewServer(port, filePath, bufferSize)
	log.Printf("Starting broker on :%d", port)
	err := server.Start()
	if err != nil {
		log.Fatalf("Failed to start server: %w", err)
	}
}
