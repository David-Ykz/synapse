package main

import (
	"encoding/json"
	"fmt"
	"net/http"
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

func startMetricsServer(s *broker.Server, port int, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/lag", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snapshots := s.GetLagSnapshot()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snapshots)
	})
	addr := fmt.Sprintf(":%d", port)
	logger.Info("started metrics server", zap.String("addr", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Fatal("metrics server failed", zap.Error(err))
	}
}

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	port, _ := strconv.Atoi(getEnvOrDefault("BROKER_PORT", "8080", logger))
	bufferSize, _ := strconv.Atoi(getEnvOrDefault("BROKER_BUFFER_SIZE", "4096", logger))
	filePath := getEnvOrDefault("BROKER_FILEPATH", "data", logger)

	// Raft specific Envs
	nodeID := getEnvOrDefault("NODE_ID", "synapse-broker-0", logger)
	headlessSvc := getEnvOrDefault("HEADLESS_SVC", "synapse-broker-headless.default.svc.cluster.local", logger)
	raftPort := getEnvOrDefault("RAFT_PORT", "8081", logger)
	replicasStr := getEnvOrDefault("REPLICAS", "3", logger)
	statefulSetName := getEnvOrDefault("STATEFULSET_NAME", "synapse-broker", logger)

	replicas, _ := strconv.Atoi(replicasStr)
	raftBindAddr := fmt.Sprintf("0.0.0.0:%s", raftPort)

	myFQDN := fmt.Sprintf("%s.%s", nodeID, headlessSvc)
	myAdvertiseAddr := fmt.Sprintf("%s:%s", myFQDN, raftPort)

	// build expected peer hostnames (Mapping ID -> Address)
	peers := make(map[string]string)
	for i := 0; i < replicas; i++ {
		peerID := fmt.Sprintf("%s-%d.%s", statefulSetName, i, headlessSvc)
		peerAddr := fmt.Sprintf("%s:%s", peerID, raftPort)
		peers[peerID] = peerAddr
	}

	metricsPort, _ := strconv.Atoi(getEnvOrDefault("METRICS_PORT", "8082", logger))

	server := broker.NewServer(port, filePath, bufferSize, logger)

	// initialize Raft with the explicitly separated addresses
	err := server.SetupRaft(myFQDN, myAdvertiseAddr, raftBindAddr, peers)
	if err != nil {
		logger.Fatal("Failed to setup Raft", zap.Error(err))
	}

	go startMetricsServer(server, metricsPort, logger)

	err = server.Start()
	if err != nil {
		logger.Error("failed to start server", zap.Error(err))
	}
}
