package state

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	client "synapse/client"
)

type AgentStatus string

const (
	StatusPending AgentStatus = "pending"
	StatusRunning AgentStatus = "running"
	StatusDone    AgentStatus = "done"
	StatusError   AgentStatus = "error"
)

type AgentTask struct {
	Name      string      `json:"name"`
	Namespace string      `json:"namespace"`
	Status    AgentStatus `json:"status"`
	Output    string      `json:"output,omitempty"`
	Error     string      `json:"error,omitempty"`
}

type RequestState struct {
	RequestID  string         `json:"request_id"`
	UserPrompt string         `json:"user_prompt"`
	Agents     []AgentTask    `json:"agents"`
	Context    map[string]any `json:"context"`
	Status     AgentStatus    `json:"status"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// AllTerminal returns true when every agent has reached a terminal state (done or error).
func (rs *RequestState) AllTerminal() bool {
	for _, a := range rs.Agents {
		if a.Status == StatusPending || a.Status == StatusRunning {
			return false
		}
	}
	return len(rs.Agents) > 0
}

func (rs *RequestState) UpdateAgent(name string, status AgentStatus, output, errMsg string) {
	for i := range rs.Agents {
		if rs.Agents[i].Name == name {
			rs.Agents[i].Status = status
			rs.Agents[i].Output = output
			rs.Agents[i].Error = errMsg
			return
		}
	}
}

// Store persists RequestState in the broker's key-value state store, keyed "request:<id>"
// There's no per-call TTL field in the state wire protocol; the server enforces its own default TTL on stored keys
type Store struct {
	client *client.StateClient
	mu     sync.Mutex
}

func NewStore(host string, port int) *Store {
	return &Store{client: client.NewStateClient(host, port)}
}

// Connect connects the underlying state client to the broker
func (s *Store) Connect() error {
	return s.client.Connect()
}

// Disconnect closes the underlying state client's connection
func (s *Store) Disconnect() error {
	return s.client.Disconnect()
}

func (s *Store) Save(ctx context.Context, rs *RequestState) error {
	rs.UpdatedAt = time.Now()
	data, err := json.Marshal(rs)
	if err != nil {
		return fmt.Errorf("marshal request state: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client.Set("request:"+rs.RequestID, data)
}

func (s *Store) Load(ctx context.Context, requestID string) (*RequestState, error) {
	s.mu.Lock()
	data, err := s.client.Get("request:" + requestID)
	s.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("load request state %s: %w", requestID, err)
	}
	var rs RequestState
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil, fmt.Errorf("unmarshal request state: %w", err)
	}
	return &rs, nil
}

func (s *Store) Delete(ctx context.Context, requestID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client.Delete("request:" + requestID)
}
