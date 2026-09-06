package synapse

import (
	"encoding/json"
	"fmt"

	common "synapse/common"
)

// ProduceEnvelope JSON-encodes env and produces it — a free function since Go can't put a type parameter on a method
func ProduceEnvelope[T any](p *Producer, env common.Envelope[T]) error {
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	p.Produce(data)
	return nil
}

/*
EnvelopeEvent mirrors Event with the payload decoded as Envelope[T]
Error: the connection failed and Subscribe's goroutine already exited. There's no message to resolve, and Ack/Nack here would block forever
DecodeError: a message was delivered but wasn't valid JSON. Unlike Error, this must still be resolved with a Nack
*/
type EnvelopeEvent[T any] struct {
	Envelope    common.Envelope[T]
	Error       error
	DecodeError error
}

// SubscribeEnvelope wraps Consumer's Subscribe, decoding each payload as Envelope[T]. Note that it never calls Ack/Nack itself
func SubscribeEnvelope[T any](c *Consumer) <-chan EnvelopeEvent[T] {
	raw := c.Subscribe()
	out := make(chan EnvelopeEvent[T])
	go func() {
		defer close(out)
		for event := range raw {
			if event.Error != nil {
				out <- EnvelopeEvent[T]{Error: event.Error}
				continue
			}
			var env common.Envelope[T]
			if err := json.Unmarshal(event.Payload, &env); err != nil {
				out <- EnvelopeEvent[T]{DecodeError: fmt.Errorf("unmarshal envelope: %w", err)}
				continue
			}
			out <- EnvelopeEvent[T]{Envelope: env}
		}
	}()
	return out
}
