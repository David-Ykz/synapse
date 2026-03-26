package client

import (
	"context"
	"fmt"
	"net"
	"synapse/common"
)

type Producer struct {
	common.Config
	ctx             context.Context
	connection      net.Conn
	producerChannel chan []byte
}

/* Initializes a new Producer */
func NewProducer(config common.Config) *Producer {
	return &Producer{
		Config:          config,
		ctx:             context.Background(),
		producerChannel: make(chan []byte, config.ChannelSize),
	}
}

/* Connects to the broker */
func (p *Producer) Connect() error {
	addr := net.JoinHostPort(p.Host, fmt.Sprintf("%d", p.Port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("Producer.Connect() failed to connect to broker at address %s: %w", addr, err)
	}
	p.connection = conn

	// start up background worker to write events in the channel
	go func() {
		for payload := range p.producerChannel {
			common.WritePacket(p.connection, common.PRODUCER_MESSAGE, p.Namespace, payload)
		}
	}()

	return nil
}

/* Sends a disconnect signal to the broker and terminates the connection */
func (p *Producer) Disconnect() error {
	err := common.WritePacket(p.connection, common.DISCONNECT, p.Namespace, []byte(""))
	if err != nil {
		return fmt.Errorf("Producer.Disconnect() failed to send message to broker at namespace %s: %w", p.Namespace, err)
	}
	p.connection.Close()
	return nil
}

func (p *Producer) Produce(payload []byte) {
	p.producerChannel <- payload
}
