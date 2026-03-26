package main

import (
	"log"
	"os"
	"strconv"

	broker "github.com/David-Ykz/synapse/broker/internal"
)

const (
	BROKER_PORT         = "BROKER_PORT"
	DEFAULT_PORT        = "8080"
	BROKER_FILEPATH     = "BROKER_FILEPATH"
	DEFAULT_FILEPATH    = "data"
	BROKER_BUFFER_SIZE  = "BROKER_BUFFER_SIZE"
	DEFAULT_BUFFER_SIZE = "4096"

	MISSING_ENV_MESSAGE = "%s not found, defaulting to %d"
)

func main() {
	portStr, ok := os.LookupEnv(BROKER_PORT)
	if !ok {
		log.Printf(MISSING_ENV_MESSAGE, BROKER_PORT, DEFAULT_PORT)
	}
	filePath, ok := os.LookupEnv(BROKER_FILEPATH)
	if !ok {
		log.Printf(MISSING_ENV_MESSAGE, BROKER_FILEPATH, DEFAULT_FILEPATH)
	}
	bufferStr, ok := os.LookupEnv(BROKER_BUFFER_SIZE)
	if !ok {
		log.Printf(MISSING_ENV_MESSAGE, BROKER_BUFFER_SIZE, DEFAULT_BUFFER_SIZE)
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
