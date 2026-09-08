package supervisor

import (
	"context"
	"fmt"
	"sync"
	"time"

	client "synapse/client"
	common "synapse/common"
	"synapse/state"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// AgentDispatch is the content of the common.Envelope sent to an agent worker; the envelope's RequestID correlates it back to the originating request
type AgentDispatch struct {
	AgentName string `json:"agent_name"`
	Prompt    string `json:"prompt"`
}

// AgentResult is the content of the common.Envelope an agent worker sends back once it has finished an attempt, only for an attempt it actually completed; a transient failure worth retrying is a Consumer.Nack on the original dispatch, not an AgentResult with Error set
type AgentResult struct {
	AgentName string `json:"agent_name"`
	Result    string `json:"result"`
	Error     string `json:"error,omitempty"`
}

// PlannedAgent describes an agent task to be dispatched
type PlannedAgent struct {
	Name      string
	Namespace string // broker namespace of the target agent worker
	Prompt    string
}

// Handler is implemented by the user to define supervisor behavior
type Handler interface {
	// PlanAgents decides which agents to invoke and builds their prompts; called once when a new user request arrives
	PlanAgents(ctx context.Context, userPrompt string, rs *state.RequestState) ([]PlannedAgent, error)

	// OnAgentDone is called after each agent completes; return replacement PlannedAgents to retry/add work or nil to proceed, updating rs.Context as needed
	OnAgentDone(ctx context.Context, rs *state.RequestState, resp AgentResult) ([]PlannedAgent, error)

	// BuildFinalResponse compiles the final answer once all agents are terminal
	BuildFinalResponse(ctx context.Context, rs *state.RequestState) (string, error)
}

type Config struct {
	InputNamespace  string // where user requests arrive
	ResultNamespace string // where agent workers publish their responses
	OutputNamespace string // where final responses are published
	BrokerHost      string
	BrokerPort      int
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
		store:     state.NewStore(cfg.BrokerHost, cfg.BrokerPort),
		handler:   handler,
		logger:    logger,
		producers: make(map[string]*client.Producer),
	}
}

func (s *Supervisor) Connect() error {
	if err := s.store.Connect(); err != nil {
		return fmt.Errorf("connect state store: %w", err)
	}

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
	s.store.Disconnect()
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
		env := common.NewEnvelope(rs.RequestID, common.StatusOK, AgentDispatch{
			AgentName: a.Name,
			Prompt:    a.Prompt,
		})
		p, err := s.getProducer(a.Namespace)
		if err != nil {
			return err
		}
		if err := client.ProduceEnvelope(p, env); err != nil {
			return fmt.Errorf("produce agent dispatch: %w", err)
		}
		rs.UpdateAgent(a.Name, state.StatusRunning, "", "")
	}
	return s.store.Save(ctx, rs)
}

func (s *Supervisor) handleRequest(ctx context.Context, payload []byte) {
	requestID := uuid.New().String()
	userPrompt := string(payload)

	// The consumer connection blocks for this verdict before polling again, so every return path must resolve it exactly once
	success := false
	defer func() {
		if success {
			s.consumer.Ack()
		} else {
			s.consumer.Nack()
		}
	}()

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

	success = true
	if err := s.dispatch(ctx, rs, agents); err != nil {
		s.logger.Error("dispatch failed", zap.String("request_id", requestID), zap.Error(err))
	}
	s.logger.Info("request dispatched",
		zap.String("request_id", requestID),
		zap.Int("agents", len(agents)))
}

// handleResult processes agent responses serially to avoid concurrent writes to the same request state
func (s *Supervisor) handleResult(ctx context.Context, env common.Envelope[AgentResult]) {
	// The result consumer connection blocks for this verdict before polling again, so every return path must resolve it exactly once
	success := false
	defer func() {
		if success {
			s.resultConsumer.Ack()
		} else {
			s.resultConsumer.Nack()
		}
	}()

	resp := env.Content

	rs, err := s.store.Load(ctx, env.RequestID)
	if err != nil {
		s.logger.Error("load request state failed",
			zap.String("request_id", env.RequestID), zap.Error(err))
		return
	}

	if env.Status == common.StatusError {
		rs.UpdateAgent(resp.AgentName, state.StatusError, "", resp.Error)
	} else {
		rs.UpdateAgent(resp.AgentName, state.StatusDone, resp.Result, "")
	}

	retry, err := s.handler.OnAgentDone(ctx, rs, resp)
	if err != nil {
		s.logger.Error("OnAgentDone failed",
			zap.String("request_id", env.RequestID), zap.Error(err))
		return
	}

	if len(retry) > 0 {
		success = true
		for _, a := range retry {
			rs.Agents = append(rs.Agents, state.AgentTask{
				Name:      a.Name,
				Namespace: a.Namespace,
				Status:    state.StatusPending,
			})
		}
		if err := s.dispatch(ctx, rs, retry); err != nil {
			s.logger.Error("retry dispatch failed",
				zap.String("request_id", env.RequestID), zap.Error(err))
		}
		return
	}

	if err := s.store.Save(ctx, rs); err != nil {
		s.logger.Error("save state failed", zap.String("request_id", env.RequestID), zap.Error(err))
		return
	}

	if !rs.AllTerminal() {
		success = true
		return
	}

	finalResponse, err := s.handler.BuildFinalResponse(ctx, rs)
	if err != nil {
		s.logger.Error("BuildFinalResponse failed",
			zap.String("request_id", env.RequestID), zap.Error(err))
		return
	}

	success = true
	s.outputProducer.Produce([]byte(finalResponse))
	if err := s.store.Delete(ctx, env.RequestID); err != nil {
		s.logger.Warn("delete request state failed",
			zap.String("request_id", env.RequestID), zap.Error(err))
	}
	s.logger.Info("request completed", zap.String("request_id", env.RequestID))
}

func (s *Supervisor) Run(ctx context.Context) {
	requests := s.consumer.Subscribe()
	results := client.SubscribeEnvelope[AgentResult](s.resultConsumer)

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
			// Run in goroutine, PlanAgents may do LLM calls
			go s.handleRequest(ctx, event.Payload)
		case envEvent, ok := <-results:
			if !ok {
				return
			}
			if envEvent.Error != nil {
				// transport failure: no message to resolve, connection is dead
				s.logger.Warn("result event error", zap.Error(envEvent.Error))
				continue
			}
			if envEvent.DecodeError != nil {
				// a message WAS delivered, just not valid AgentResult JSON, still needs resolving so the broker can back off and eventually DLQ it
				s.logger.Warn("result envelope decode error", zap.Error(envEvent.DecodeError))
				s.resultConsumer.Nack()
				continue
			}
			// Processed serially to avoid concurrent writes to the same request state
			s.handleResult(ctx, envEvent.Envelope)
		}
	}
}
