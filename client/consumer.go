package synapse

import (
	"context"
	"fmt"
	"net"
	common "synapse/common"
	"time"
)

type ConsumerConfig struct {
	common.Config
	PollIntervalMs    int
	PollBackoff       bool
	MaxPollIntervalMs int
}

type Consumer struct {
	ConsumerConfig
	ctx        context.Context
	connection net.Conn
}

type Event struct {
	Payload []byte
	Error   error
}

/* Initializes a new Consumer */
func NewConsumer(config ConsumerConfig) *Consumer {
	return &Consumer{
		ConsumerConfig: config,
		ctx:            context.Background(),
	}
}

/* Connects to the broker */
func (c *Consumer) Connect() error {
	addr := net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.Port))
	var conn net.Conn
	var err error
	for i := 0; i < common.MAX_RETRIES; i++ {
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			break
		}
		time.Sleep(common.RETRY_INTERVAL_INITIALIZATION * time.Second)
	}
	if err != nil {
		return fmt.Errorf("Consumer.Connect() failed to connect to broker at address %s: %w", addr, err)
	}
	c.connection = conn
	return nil
}

/* Sends a disconnect signal to the broker and terminates the connection */
func (c *Consumer) Disconnect() error {
	err := common.WritePacket(c.connection, common.DISCONNECT, c.Namespace, []byte(""))
	if err != nil {
		return fmt.Errorf("Consumer.Disconnect() failed to send message to broker at namespace %s: %w", c.Namespace, err)
	}
	c.connection.Close()
	return nil
}

/* Polls for events and sends them through a channel when it receives one */
func (c *Consumer) Subscribe() <-chan Event {
	output := make(chan Event, c.ChannelSize)
	go func() {
		defer close(output)
		pollInterval := c.PollIntervalMs
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				// send consumer request
				var err error
				for i := 0; i < common.MAX_RETRIES; i++ {
					err = common.WritePacket(c.connection, common.CONSUMER_MESSAGE, c.Namespace, []byte(""))
					if err == nil {
						break
					}
					time.Sleep(common.RETRY_INTERVAL_RUNTIME * time.Second)
				}
				if err != nil {
					output <- Event{Error: err}
					return
				}
				// get response
				_, _, payload, err := common.ReadPacket(c.connection)
				if err != nil {
					output <- Event{Error: err}
					return
				}
				// handle data if present, otherwise backoff if specified
				if len(payload) > 0 {
					output <- Event{Payload: payload}
					pollInterval = c.PollIntervalMs
				} else if c.PollBackoff {
					pollInterval = min(pollInterval*2, c.MaxPollIntervalMs)
				}

				time.Sleep(time.Duration(pollInterval) * time.Millisecond)
			}
		}
	}()
	return output
}
