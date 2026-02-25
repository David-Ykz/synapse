package client

import (
	"context"
	"fmt"
	"net"
	"synapse/src/common"
	"time"
)

type Config struct {
	Host               string
	Port               int
	Namespace          string
	PollIntervalMs     int
	PollBackoff        bool
	MaxPollIntervalMs  int
	ConsumerBufferSize int
}

type BasicClient struct {
	config     Config
	ctx        context.Context
	connection net.Conn
}

type Event struct {
	Payload []byte
	Error   error
}

/* Initializes a new Basic Client */
func NewBasicClient(config Config) *BasicClient {
	return &BasicClient{
		config: config,
	}
}

/* Connects to the broker */
func (b *BasicClient) Connect() error {
	addr := net.JoinHostPort(b.config.Host, fmt.Sprintf("%d", b.config.Port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("BasicClient.Connect() failed to connect to broker at address %s: %w", addr, err)
	}
	b.connection = conn
	return nil
}

/* Sends a disconnect signal to the broker and terminates the connection */
func (b *BasicClient) Disconnect() error {
	err := common.WritePacket(b.connection, common.DISCONNECT, b.config.Namespace, []byte(""))
	if err != nil {
		return fmt.Errorf("BasicClient.Disconnect() failed to send message to broker at namespace %s: %w", namespace, err)
	}
	b.connection.Close()
	return nil
}

/* Polls for events and sends them through a channel when it receives one */
func (b *BasicClient) Subscribe() <-chan Event {
	output := make(chan Event, b.config.ConsumerBufferSize)
	go func() {
		defer close(output)
		pollInterval := b.config.PollIntervalMs
		for {
			select {
			case <-b.ctx.Done():
				return
			default:
				// send consumer request
				err := common.WritePacket(b.connection, common.CONSUMER_MESSAGE, b.config.Namespace, []byte(""))
				if err != nil {
					output <- Event{Error: err}
					return
				}
				// get response
				_, _, payload, err := common.ReadPacket(b.connection)
				if err != nil {
					output <- Event{Error: err}
					return
				}
				// handle data if present, otherwise backoff if specified
				if len(payload) > 0 {
					output <- Event{Payload: payload}
					pollInterval = b.config.PollIntervalMs
				} else {
					pollInterval = min(pollInterval*2, b.config.MaxPollIntervalMs)
				}

				time.Sleep(time.Duration(pollInterval) * time.Millisecond)
			}
		}
	}()
	return output
}

func (b *BasicClient) Produce(namespace string, payload []byte) error {
	err := common.WritePacket(b.connection, common.PRODUCER_MESSAGE, namespace, payload)
	if err != nil {
		return fmt.Errorf("BasicClient.Produce() failed to write packet: %w", err)
	}
	return nil
}
