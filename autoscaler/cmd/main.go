package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	autoscaler "synapse/autoscaler/internal"

	"go.uber.org/zap"
)

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	brokerAddr := getEnv("BROKER_METRICS_ADDR", "synapse-broker-client:8082")
	pollSec, _ := strconv.Atoi(getEnv("POLL_INTERVAL_SEC", "15"))
	cooldownSec, _ := strconv.Atoi(getEnv("COOLDOWN_SEC", "60"))
	k8sNs := getEnv("K8S_NAMESPACE", "default")
	agentCfgJSON := os.Getenv("AGENT_CONFIGS")

	var agents []autoscaler.AgentScalingConfig
	if err := json.Unmarshal([]byte(agentCfgJSON), &agents); err != nil {
		logger.Fatal("failed to parse AGENT_CONFIGS", zap.Error(err))
	}

	cfg := autoscaler.ScalerConfig{
		BrokerMetricsAddr: brokerAddr,
		PollIntervalSec:   pollSec,
		CooldownSec:       cooldownSec,
		K8sNamespace:      k8sNs,
		Agents:            agents,
	}

	scaler, err := autoscaler.NewScaler(cfg, logger)
	if err != nil {
		logger.Fatal("failed to create scaler", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	scaler.Run(ctx)
}
