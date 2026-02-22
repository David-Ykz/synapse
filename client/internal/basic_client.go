package client

import (
	"fmt"
	"net"
	"synapse/common"
	"time"
)

type Config struct {
	Host              string
	Port              int
	DefaultNamespace  string
	PollIntervalMs    int
	PollBackoff       bool
	MaxPollIntervalMs int
	MaxRetry          int
}

type BasicClient struct {
	config     Config
	connection net.Conn
}

func NewBasicClient(config Config) *BasicClient {
	return &BasicClient{
		config: config,
	}
}

func (b *BasicClient) Connect() error {
	addr := net.JoinHostPort(b.config.Host, fmt.Sprintf("%d", b.config.Port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("BasicClient.Connect() failed to connect to broker at address %s: %w", addr, err)
	}
	b.connection = conn
	return nil
}

func (b *BasicClient) Disconnect(namespace string) error {
	if namespace == "" {
		namespace = b.config.DefaultNamespace
	}
	err := common.WritePacket(b.connection, common.DISCONNECT, namespace, []byte(""))
	if err != nil {
		return fmt.Errorf("BasicClient.Disconnect() failed to send message to broker at namespace %s: %w", namespace, err)
	}
	b.connection.Close()
	return nil
}

func (b *BasicClient) Consume(namespace string) (payload []byte, err error) {
	pollInterval := b.config.PollIntervalMs
	for {
		err = common.WritePacket(b.connection, common.CONSUMER_MESSAGE, namespace, []byte(""))
		if err != nil {
			err = fmt.Errorf("BasicClient.Consume() failed to send poll request: %w", err)
			return
		}

		_, _, payload, err = common.ReadPacket(b.connection)
		if err != nil {
			err = fmt.Errorf("BasicClient.Consume() failed to read poll response: %w", err)
			return
		}
		if len(payload) == 0 {
			if b.config.PollBackoff {
				if b.config.MaxPollIntervalMs < b.config.PollIntervalMs {
					fmt.Println("Max poll interval is less than default poll interval")
					return
				}
				pollInterval = min(b.config.MaxPollIntervalMs, 2*pollInterval)
			}
		} else {
			return
		}
		time.Sleep(time.Duration(pollInterval) * time.Millisecond)
	}
}

func (b *BasicClient) Produce(namespace string, payload []byte) error {
	err := common.WritePacket(b.connection, common.PRODUCER_MESSAGE, namespace, payload)
	if err != nil {
		return fmt.Errorf("BasicClient.Produce() failed to write packet: %w", err)
	}
	return nil
}
