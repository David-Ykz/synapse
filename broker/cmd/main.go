package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	broker "synapse/broker/internal"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	BROKER_PORT         = "BROKER_PORT"
	DEFAULT_PORT        = "8080"
	BROKER_FILEPATH     = "BROKER_FILEPATH"
	DEFAULT_FILEPATH    = "data"
	BROKER_BUFFER_SIZE  = "BROKER_BUFFER_SIZE"
	DEFAULT_BUFFER_SIZE = "4096"

	BROKER_MAX_RETRIES      = "BROKER_MAX_RETRIES"
	DEFAULT_MAX_RETRIES     = "5"
	BROKER_BACKOFF_BASE_MS  = "BROKER_BACKOFF_BASE_MS"
	DEFAULT_BACKOFF_BASE_MS = "500"
	BROKER_BACKOFF_MAX_MS   = "BROKER_BACKOFF_MAX_MS"
	DEFAULT_BACKOFF_MAX_MS  = "30000"

	STATE_MAX_ENTRIES               = "STATE_MAX_ENTRIES"
	DEFAULT_STATE_MAX_ENTRIES       = "100000"
	STATE_MAX_VALUE_BYTES           = "STATE_MAX_VALUE_BYTES"
	DEFAULT_STATE_MAX_VALUE_BYTES   = "1048576" // 1 MB
	STATE_DEFAULT_TTL_MS            = "STATE_DEFAULT_TTL_MS"
	DEFAULT_STATE_DEFAULT_TTL_MS    = "3600000"
	STATE_SWEEP_INTERVAL_MS         = "STATE_SWEEP_INTERVAL_MS"
	DEFAULT_STATE_SWEEP_INTERVAL_MS = "30000"

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

	registry := prometheus.NewRegistry()
	registry.MustRegister(broker.NewLagCollector(s))
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

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

	maxRetries, _ := strconv.Atoi(getEnvOrDefault(BROKER_MAX_RETRIES, DEFAULT_MAX_RETRIES, logger))
	backoffBaseMs, _ := strconv.Atoi(getEnvOrDefault(BROKER_BACKOFF_BASE_MS, DEFAULT_BACKOFF_BASE_MS, logger))
	backoffMaxMs, _ := strconv.Atoi(getEnvOrDefault(BROKER_BACKOFF_MAX_MS, DEFAULT_BACKOFF_MAX_MS, logger))
	backoffBase := time.Duration(backoffBaseMs) * time.Millisecond
	backoffMax := time.Duration(backoffMaxMs) * time.Millisecond

	stateMaxEntries, _ := strconv.Atoi(getEnvOrDefault(STATE_MAX_ENTRIES, DEFAULT_STATE_MAX_ENTRIES, logger))
	stateMaxValueBytes, _ := strconv.Atoi(getEnvOrDefault(STATE_MAX_VALUE_BYTES, DEFAULT_STATE_MAX_VALUE_BYTES, logger))
	stateDefaultTTLMs, _ := strconv.Atoi(getEnvOrDefault(STATE_DEFAULT_TTL_MS, DEFAULT_STATE_DEFAULT_TTL_MS, logger))
	stateSweepIntervalMs, _ := strconv.Atoi(getEnvOrDefault(STATE_SWEEP_INTERVAL_MS, DEFAULT_STATE_SWEEP_INTERVAL_MS, logger))
	stateDefaultTTL := time.Duration(stateDefaultTTLMs) * time.Millisecond
	stateSweepInterval := time.Duration(stateSweepIntervalMs) * time.Millisecond

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

	server := broker.NewServer(port, filePath, bufferSize, maxRetries, backoffBase, backoffMax, stateMaxEntries, stateMaxValueBytes, stateDefaultTTL, stateSweepInterval, logger)

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
