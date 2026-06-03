package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
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

type Store struct {
	client *redis.Client
	ttl    time.Duration
}

func NewStore(addr string, ttl time.Duration) *Store {
	return &Store{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		ttl:    ttl,
	}
}

func (s *Store) Save(ctx context.Context, rs *RequestState) error {
	rs.UpdatedAt = time.Now()
	data, err := json.Marshal(rs)
	if err != nil {
		return fmt.Errorf("marshal request state: %w", err)
	}
	return s.client.Set(ctx, "request:"+rs.RequestID, data, s.ttl).Err()
}

func (s *Store) Load(ctx context.Context, requestID string) (*RequestState, error) {
	data, err := s.client.Get(ctx, "request:"+requestID).Bytes()
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
	return s.client.Del(ctx, "request:"+requestID).Err()
}
