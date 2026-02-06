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
	PacketType        common.PacketType
	DefaultNamespace  string
	BatchSize         int
	PollIntervalMs    int
	PollBackoff       bool
	MaxPollIntervalMs int
	MaxRetry          int
}

type Client struct {
	config     Config
	connection net.Conn
}

func NewClient(config Config) *Client {
	return &Client{
		config: config,
	}
}

func (p *Client) Connect() error {
	addr := net.JoinHostPort(p.config.Host, fmt.Sprintf("%d", p.config.Port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("error connecting to broker: %w", err)
	}
	p.connection = conn
	return nil
}

func (p *Client) Send(payload string, namespace string) error {
	if namespace == "" {
		namespace = p.config.DefaultNamespace
	}
	err := common.WritePacket(p.connection, p.config.PacketType, namespace, payload)
	if err != nil {
		return fmt.Errorf("error sending message to broker: %w", err)
	}
	return nil
}

func (p *Client) Disconnect(namespace string) error {
	if namespace == "" {
		namespace = p.config.DefaultNamespace
	}
	err := common.WritePacket(p.connection, common.DISCONNECT, namespace, "")
	if err != nil {
		return fmt.Errorf("error sending message to broker: %w", err)
	}
	p.connection.Close()
	return nil
}

func (p *Client) Poll(namespace string) (payload string, err error) {
	pollInterval := p.config.PollIntervalMs
	for {
		err = p.Send("", namespace)
		if err != nil {
			return
		}

		_, _, payload, err = common.ReadPacket(p.connection)
		if err != nil {
			return
		}
		if payload == "" {
			fmt.Println("No message")
			if p.config.PollBackoff {
				if p.config.MaxPollIntervalMs < p.config.PollIntervalMs {
					fmt.Println("Max poll interval is less than default poll interval")
					return
				}
				pollInterval = min(p.config.MaxPollIntervalMs, 2*pollInterval)
			}
		} else {
			fmt.Println("Received message:", payload)
			pollInterval = p.config.PollIntervalMs
		}
		time.Sleep(time.Duration(pollInterval) * time.Millisecond)
	}
}

func (p *Client) Push(namespace string, data string) error {
	return p.Send(data, namespace)
}
