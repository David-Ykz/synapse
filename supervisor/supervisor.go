package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	client "github.com/David-Ykz/synapse/client"
	common "github.com/David-Ykz/synapse/common"
	"github.com/David-Ykz/synapse/state"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// AgentEvent is the message dispatched to an agent worker.
type AgentEvent struct {
	RequestID string `json:"request_id"`
	AgentName string `json:"agent_name"`
	Prompt    string `json:"prompt"`
}

// AgentResponse is the message an agent worker sends back to the supervisor.
type AgentResponse struct {
	RequestID string `json:"request_id"`
	AgentName string `json:"agent_name"`
	Result    string `json:"result"`
	Error     string `json:"error,omitempty"`
}

// PlannedAgent describes an agent task to be dispatched.
type PlannedAgent struct {
	Name      string
	Namespace string // broker namespace of the target agent worker
	Prompt    string
}

// Handler is implemented by the user to define supervisor behavior.
type Handler interface {
	// PlanAgents decides which agents to invoke and builds their prompts.
	// Called once when a new user request arrives.
	PlanAgents(ctx context.Context, userPrompt string, rs *state.RequestState) ([]PlannedAgent, error)

	// OnAgentDone is called after each agent completes (success or error).
	// Return replacement PlannedAgents to retry or add new work; return nil to proceed.
	// Implementations should update rs.Context with any relevant output.
	OnAgentDone(ctx context.Context, rs *state.RequestState, resp AgentResponse) ([]PlannedAgent, error)

	// BuildFinalResponse compiles the final answer once all agents are terminal.
	BuildFinalResponse(ctx context.Context, rs *state.RequestState) (string, error)
}

type Config struct {
	// InputNamespace is where user requests arrive.
	InputNamespace string
	// ResultNamespace is where agent workers publish their responses.
	ResultNamespace string
	// OutputNamespace is where final responses are published.
	OutputNamespace string
	BrokerHost      string
	BrokerPort      int
	RedisAddr       string
	RequestTTL      time.Duration
}

type Supervisor struct {
	cfg            Config
	store          *state.Store
	handler        Handler
	logger         *zap.Logger
	consumer       *client.Consumer
	resultConsumer *client.Consumer
	outputProducer *client.Producer
	producers      map[string]*client.Producer
	producersMu    sync.Mutex
}

func New(cfg Config, handler Handler, logger *zap.Logger) *Supervisor {
	return &Supervisor{
		cfg:       cfg,
		store:     state.NewStore(cfg.RedisAddr, cfg.RequestTTL),
		handler:   handler,
		logger:    logger,
		producers: make(map[string]*client.Producer),
	}
}

func (s *Supervisor) Connect() error {
	s.consumer = client.NewConsumer(client.ConsumerConfig{
		Config: common.Config{
			Host:        s.cfg.BrokerHost,
			Port:        s.cfg.BrokerPort,
			Namespace:   s.cfg.InputNamespace,
			ChannelSize: 128,
		},
		PollIntervalMs:    100,
		PollBackoff:       true,
		MaxPollIntervalMs: 5000,
	})
	if err := s.consumer.Connect(); err != nil {
		return fmt.Errorf("connect input consumer: %w", err)
	}

	s.resultConsumer = client.NewConsumer(client.ConsumerConfig{
		Config: common.Config{
			Host:        s.cfg.BrokerHost,
			Port:        s.cfg.BrokerPort,
			Namespace:   s.cfg.ResultNamespace,
			ChannelSize: 128,
		},
		PollIntervalMs:    100,
		PollBackoff:       true,
		MaxPollIntervalMs: 5000,
	})
	if err := s.resultConsumer.Connect(); err != nil {
		return fmt.Errorf("connect result consumer: %w", err)
	}

	s.outputProducer = client.NewProducer(common.Config{
		Host:        s.cfg.BrokerHost,
		Port:        s.cfg.BrokerPort,
		Namespace:   s.cfg.OutputNamespace,
		ChannelSize: 128,
	})
	if err := s.outputProducer.Connect(); err != nil {
		return fmt.Errorf("connect output producer: %w", err)
	}

	return nil
}

func (s *Supervisor) Disconnect() {
	s.consumer.Disconnect()
	s.resultConsumer.Disconnect()
	s.outputProducer.Disconnect()
	s.producersMu.Lock()
	defer s.producersMu.Unlock()
	for _, p := range s.producers {
		p.Disconnect()
	}
}

func (s *Supervisor) getProducer(namespace string) (*client.Producer, error) {
	s.producersMu.Lock()
	defer s.producersMu.Unlock()
	if p, ok := s.producers[namespace]; ok {
		return p, nil
	}
	p := client.NewProducer(common.Config{
		Host:        s.cfg.BrokerHost,
		Port:        s.cfg.BrokerPort,
		Namespace:   namespace,
		ChannelSize: 128,
	})
	if err := p.Connect(); err != nil {
		return nil, fmt.Errorf("connect producer for %s: %w", namespace, err)
	}
	s.producers[namespace] = p
	return p, nil
}

func (s *Supervisor) dispatch(ctx context.Context, rs *state.RequestState, agents []PlannedAgent) error {
	for _, a := range agents {
		evt := AgentEvent{
			RequestID: rs.RequestID,
			AgentName: a.Name,
			Prompt:    a.Prompt,
		}
		data, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal agent event: %w", err)
		}
		p, err := s.getProducer(a.Namespace)
		if err != nil {
			return err
		}
		p.Produce(data)
		rs.UpdateAgent(a.Name, state.StatusRunning, "", "")
	}
	return s.store.Save(ctx, rs)
}

func (s *Supervisor) handleRequest(ctx context.Context, payload []byte) {
	requestID := uuid.New().String()
	userPrompt := string(payload)

	rs := &state.RequestState{
		RequestID:  requestID,
		UserPrompt: userPrompt,
		Agents:     []state.AgentTask{},
		Context:    map[string]any{},
		Status:     state.StatusRunning,
		CreatedAt:  time.Now(),
	}

	agents, err := s.handler.PlanAgents(ctx, userPrompt, rs)
	if err != nil {
		s.logger.Error("PlanAgents failed", zap.String("request_id", requestID), zap.Error(err))
		return
	}

	for _, a := range agents {
		rs.Agents = append(rs.Agents, state.AgentTask{
			Name:      a.Name,
			Namespace: a.Namespace,
			Status:    state.StatusPending,
		})
	}

	if err := s.store.Save(ctx, rs); err != nil {
		s.logger.Error("save initial state failed", zap.String("request_id", requestID), zap.Error(err))
		return
	}

	if err := s.dispatch(ctx, rs, agents); err != nil {
		s.logger.Error("dispatch failed", zap.String("request_id", requestID), zap.Error(err))
	}
	s.logger.Info("request dispatched",
		zap.String("request_id", requestID),
		zap.Int("agents", len(agents)))
}

// handleResult processes agent responses serially to avoid concurrent writes to the same request state.
func (s *Supervisor) handleResult(ctx context.Context, payload []byte) {
	var resp AgentResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		s.logger.Error("unmarshal agent response failed", zap.Error(err))
		return
	}

	rs, err := s.store.Load(ctx, resp.RequestID)
	if err != nil {
		s.logger.Error("load request state failed",
			zap.String("request_id", resp.RequestID), zap.Error(err))
		return
	}

	if resp.Error != "" {
		rs.UpdateAgent(resp.AgentName, state.StatusError, "", resp.Error)
	} else {
		rs.UpdateAgent(resp.AgentName, state.StatusDone, resp.Result, "")
	}

	retry, err := s.handler.OnAgentDone(ctx, rs, resp)
	if err != nil {
		s.logger.Error("OnAgentDone failed",
			zap.String("request_id", resp.RequestID), zap.Error(err))
		return
	}

	if len(retry) > 0 {
		for _, a := range retry {
			rs.Agents = append(rs.Agents, state.AgentTask{
				Name:      a.Name,
				Namespace: a.Namespace,
				Status:    state.StatusPending,
			})
		}
		if err := s.dispatch(ctx, rs, retry); err != nil {
			s.logger.Error("retry dispatch failed",
				zap.String("request_id", resp.RequestID), zap.Error(err))
		}
		return
	}

	if err := s.store.Save(ctx, rs); err != nil {
		s.logger.Error("save state failed", zap.String("request_id", resp.RequestID), zap.Error(err))
		return
	}

	if !rs.AllTerminal() {
		return
	}

	finalResponse, err := s.handler.BuildFinalResponse(ctx, rs)
	if err != nil {
		s.logger.Error("BuildFinalResponse failed",
			zap.String("request_id", resp.RequestID), zap.Error(err))
		return
	}

	s.outputProducer.Produce([]byte(finalResponse))
	if err := s.store.Delete(ctx, resp.RequestID); err != nil {
		s.logger.Warn("delete request state failed",
			zap.String("request_id", resp.RequestID), zap.Error(err))
	}
	s.logger.Info("request completed", zap.String("request_id", resp.RequestID))
}

func (s *Supervisor) Run(ctx context.Context) {
	requests := s.consumer.Subscribe()
	results := s.resultConsumer.Subscribe()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-requests:
			if !ok {
				return
			}
			if event.Error != nil {
				s.logger.Warn("request event error", zap.Error(event.Error))
				continue
			}
			// Run in goroutine — PlanAgents may do LLM calls.
			go s.handleRequest(ctx, event.Payload)
		case event, ok := <-results:
			if !ok {
				return
			}
			if event.Error != nil {
				s.logger.Warn("result event error", zap.Error(event.Error))
				continue
			}
			// Processed serially to avoid concurrent writes to the same request state.
			s.handleResult(ctx, event.Payload)
		}
	}
}
