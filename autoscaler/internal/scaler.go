package autoscaler

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type AgentScalingConfig struct {
	DeploymentName string `json:"deployment_name"`
	Namespace      string `json:"input_namespace"`
	MinReplicas    int    `json:"min_replicas"`
	MaxReplicas    int    `json:"max_replicas"`
	LagPerReplica  int    `json:"lag_per_replica"`
}

type ScalerConfig struct {
	BrokerMetricsAddr string
	PollIntervalSec   int
	CooldownSec       int
	K8sNamespace      string
	Agents            []AgentScalingConfig
}

type agentState struct {
	currentReplicas int32
	lastScaleTime   time.Time
}

type Scaler struct {
	config      ScalerConfig
	metrics     *MetricsClient
	k8s         *kubernetes.Clientset
	logger      *zap.Logger
	agentStates map[string]*agentState
}

func NewScaler(cfg ScalerConfig, logger *zap.Logger) (*Scaler, error) {
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("k8s in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		return nil, fmt.Errorf("k8s clientset: %w", err)
	}
	states := make(map[string]*agentState, len(cfg.Agents))
	for _, a := range cfg.Agents {
		states[a.DeploymentName] = &agentState{}
	}
	return &Scaler{
		config:      cfg,
		metrics:     NewMetricsClient(cfg.BrokerMetricsAddr),
		k8s:         clientset,
		logger:      logger,
		agentStates: states,
	}, nil
}

// desiredReplicas computes target replica count: clamp(ceiling(lag / lagPerReplica), min, max)
func desiredReplicas(lag int64, lagPerReplica, minR, maxR int) int32 {
	if lag == 0 {
		return int32(minR)
	}
	computed := int(math.Ceil(float64(lag) / float64(lagPerReplica)))
	if computed < minR {
		computed = minR
	}
	if computed > maxR {
		computed = maxR
	}
	return int32(computed)
}

func (s *Scaler) patchReplicas(ctx context.Context, deploymentName string, replicas int32) error {
	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err := s.k8s.AppsV1().Deployments(s.config.K8sNamespace).Patch(
		ctx,
		deploymentName,
		types.MergePatchType,
		[]byte(patch),
		metav1.PatchOptions{},
	)
	return err
}

func (s *Scaler) getCurrentReplicas(ctx context.Context, deploymentName string) (int32, error) {
	d, err := s.k8s.AppsV1().Deployments(s.config.K8sNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	if d.Spec.Replicas == nil {
		return 1, nil
	}
	return *d.Spec.Replicas, nil
}

func (s *Scaler) RunOnce(ctx context.Context) {
	lagMap, err := s.metrics.FetchLag()
	if err != nil {
		s.logger.Error("failed to fetch lag", zap.Error(err))
		return
	}

	now := time.Now()
	cooldown := time.Duration(s.config.CooldownSec) * time.Second

	for _, agent := range s.config.Agents {
		lag := lagMap[agent.Namespace] // defaults to 0 if namespace not yet seen

		state := s.agentStates[agent.DeploymentName]

		// seed current replicas on first poll
		if state.currentReplicas == 0 {
			cur, err := s.getCurrentReplicas(ctx, agent.DeploymentName)
			if err != nil {
				s.logger.Error("failed to get current replicas",
					zap.String("deployment", agent.DeploymentName), zap.Error(err))
				continue
			}
			state.currentReplicas = cur
		}

		desired := desiredReplicas(lag, agent.LagPerReplica, agent.MinReplicas, agent.MaxReplicas)

		if desired == state.currentReplicas {
			continue
		}

		// scale-down respects cooldown; scale-up is immediate
		if desired < state.currentReplicas && now.Sub(state.lastScaleTime) < cooldown {
			s.logger.Info("scale-down suppressed by cooldown",
				zap.String("deployment", agent.DeploymentName),
				zap.Duration("remaining", cooldown-now.Sub(state.lastScaleTime)))
			continue
		}

		if err := s.patchReplicas(ctx, agent.DeploymentName, desired); err != nil {
			s.logger.Error("failed to patch replicas",
				zap.String("deployment", agent.DeploymentName), zap.Error(err))
			continue
		}

		s.logger.Info("scaled deployment",
			zap.String("deployment", agent.DeploymentName),
			zap.Int64("lag", lag),
			zap.Int32("from", state.currentReplicas),
			zap.Int32("to", desired))

		state.currentReplicas = desired
		state.lastScaleTime = now
	}
}

func (s *Scaler) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(s.config.PollIntervalSec) * time.Second)
	defer ticker.Stop()
	s.logger.Info("autoscaler started",
		zap.String("broker", s.config.BrokerMetricsAddr),
		zap.Int("poll_interval_sec", s.config.PollIntervalSec))
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.RunOnce(ctx)
		}
	}
}
