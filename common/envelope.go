package synapse

import "github.com/google/uuid"

type Status string // used by agents

const (
	StatusOK    Status = "ok"
	StatusError Status = "error"
)

// Envelope is a standard structured message format for agents to exchange messages over the broker
type Envelope[T any] struct {
	ID        string `json:"id"`
	RequestID string `json:"request_id"`
	Status    Status `json:"status"`
	Content   T      `json:"content"`
}

func NewEnvelope[T any](requestID string, status Status, content T) Envelope[T] {
	return Envelope[T]{
		ID:        uuid.New().String(),
		RequestID: requestID,
		Status:    status,
		Content:   content,
	}
}
