package main

import (
	"os"
	"strconv"

	broker "synapse/broker/internal"

	"go.uber.org/zap"
)

const (
	BROKER_PORT         = "BROKER_PORT"
	DEFAULT_PORT        = "8080"
	BROKER_FILEPATH     = "BROKER_FILEPATH"
	DEFAULT_FILEPATH    = "data"
	BROKER_BUFFER_SIZE  = "BROKER_BUFFER_SIZE"
	DEFAULT_BUFFER_SIZE = "4096"

	MISSING_ENV_MESSAGE = "%s not found, defaulting to %s"
)

func getEnvOrDefault(key, defaultValue string, logger *zap.Logger) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		logger.Warn("environment value not found, using default value instead",
			zap.String("key", key),
			zap.String("default", defaultValue),
		)
		return defaultValue
	}
	return value
}

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	port, _ := strconv.Atoi(getEnvOrDefault(BROKER_PORT, DEFAULT_PORT, logger))
	bufferSize, _ := strconv.Atoi(getEnvOrDefault(BROKER_BUFFER_SIZE, DEFAULT_BUFFER_SIZE, logger))
	filePath := getEnvOrDefault(BROKER_FILEPATH, DEFAULT_FILEPATH, logger)

	server := broker.NewServer(port, filePath, bufferSize, logger)
	err := server.Start()
	if err != nil {
		logger.Error("failed to start server", zap.Error(err))
	}
}
