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
	ctx               context.Context
	connection        net.Conn
	resolutionChannel chan bool // true = ack, false = nack
}

type Event struct {
	Payload []byte
	Error   error
}

/* Initializes a new Consumer */
func NewConsumer(config ConsumerConfig) *Consumer {
	return &Consumer{
		ConsumerConfig:    config,
		ctx:               context.Background(),
		resolutionChannel: make(chan bool),
	}
}

func (c *Consumer) Ack() {
	c.resolutionChannel <- true
}

func (c *Consumer) Nack() {
	c.resolutionChannel <- false
}

/* Connects to the broker */
func (c *Consumer) Connect() error {
	addr := net.JoinHostPort(c.Host, fmt.Sprintf("%d", c.Port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to broker at address %s: %w", addr, err)
	}
	c.connection = conn
	return nil
}

/* Sends a disconnect signal to the broker and terminates the connection */
func (c *Consumer) Disconnect() error {
	err := common.WritePacket(c.connection, common.DISCONNECT, c.Namespace, []byte(""))
	if err != nil {
		return fmt.Errorf("failed to send message to broker at namespace %s: %w", c.Namespace, err)
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
				err := common.WritePacket(c.connection, common.CONSUMER_MESSAGE, c.Namespace, []byte(""))
				if err != nil {
					output <- Event{Error: err}
					return
				}
				// get response
				packetType, _, payload, err := common.ReadPacket(c.connection)
				if err != nil {
					output <- Event{Error: err}
					return
				}
				// SERVER_ERROR carries a non-empty diagnostic string too, so packet type is what distinguishes a real message from "queue empty"/not-leader/etc
				if packetType == common.SERVER_MESSAGE {
					output <- Event{Payload: payload}

					// wait for the caller to Ack/Nack before polling again — at most one message is in flight on this connection at a time
					var acked bool
					select {
					case <-c.ctx.Done():
						return
					case acked = <-c.resolutionChannel:
					}

					resolutionType := common.CONSUMER_NACK
					if acked {
						resolutionType = common.CONSUMER_ACK
					}
					if err := common.WritePacket(c.connection, resolutionType, c.Namespace, []byte("")); err != nil {
						output <- Event{Error: err}
						return
					}

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
