package synapse

import (
	"context"
	"errors"
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
	conn, err := dial(c.Host, c.Port)
	if err != nil {
		return err
	}
	c.connection = conn
	return nil
}

// reconnect closes the current connection and opens a fresh one to the same address; used to retry
// against a different broker pod after ERR_NOT_LEADER, since a k8s Service load-balances new
// connections across replicas but the wire protocol itself has no leader-redirect of its own
func (c *Consumer) reconnect() error {
	if c.connection != nil {
		c.connection.Close()
	}
	conn, err := dial(c.Host, c.Port)
	if err != nil {
		return err
	}
	c.connection = conn
	return nil
}

// poll sends one CONSUMER_MESSAGE request and returns the decoded response; a not-leader rejection
// is reported as ErrNotLeader so Subscribe can reconnect and retry instead of treating it as fatal
func (c *Consumer) poll() (common.PacketType, []byte, error) {
	c.connection.SetDeadline(time.Now().Add(requestTimeout))
	if err := common.WritePacket(c.connection, common.CONSUMER_MESSAGE, c.Namespace, []byte("")); err != nil {
		return 0, nil, err
	}
	packetType, _, payload, err := common.ReadPacket(c.connection)
	if err != nil {
		return 0, nil, err
	}
	// clear the deadline once the round trip is done — a delivered message can sit waiting on the
	// caller's Ack/Nack for an arbitrary amount of time, which must not count against this connection
	c.connection.SetDeadline(time.Time{})
	if packetType == common.SERVER_ERROR && string(payload) == errNotLeaderPayload {
		return 0, nil, ErrNotLeader
	}
	return packetType, payload, nil
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
		leaderRetries := 0
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				packetType, payload, err := c.poll()
				if errors.Is(err, ErrNotLeader) {
					leaderRetries++
					if leaderRetries > maxLeaderRetries {
						output <- Event{Error: fmt.Errorf("gave up after %d leader retries: %w", maxLeaderRetries, err)}
						return
					}
					time.Sleep(leaderRetryDelay)
					if err := c.reconnect(); err != nil {
						output <- Event{Error: err}
						return
					}
					continue
				}
				if err != nil {
					output <- Event{Error: err}
					return
				}
				leaderRetries = 0
				// SERVER_ERROR carries a non-empty diagnostic string too, so packet type is what distinguishes a real message from "queue empty"/etc
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
					c.connection.SetDeadline(time.Now().Add(requestTimeout))
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
