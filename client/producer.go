package synapse

import (
	"context"
	"fmt"
	"net"
	"time"

	common "synapse/common"
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
	conn, err := dial(p.Host, p.Port)
	if err != nil {
		return err
	}
	p.connection = conn

	// start up background worker to write events in the channel
	go func() {
		for payload := range p.producerChannel {
			p.produce(payload)
		}
	}()

	return nil
}

// reconnect closes the current connection and opens a fresh one to the same address; used to retry
// against a different broker pod after ERR_NOT_LEADER, since a k8s Service load-balances new
// connections across replicas but the wire protocol itself has no leader-redirect of its own
func (p *Producer) reconnect() error {
	if p.connection != nil {
		p.connection.Close()
	}
	conn, err := dial(p.Host, p.Port)
	if err != nil {
		return err
	}
	p.connection = conn
	return nil
}

// produce sends one message, transparently reconnecting and retrying if the connected node isn't
// the raft leader; matches Produce's existing fire-and-forget contract by giving up silently once
// maxLeaderRetries is exhausted, same as any other unrecoverable error on this connection
func (p *Producer) produce(payload []byte) {
	for attempt := 0; attempt <= maxLeaderRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(leaderRetryDelay)
			if err := p.reconnect(); err != nil {
				continue
			}
		}
		p.connection.SetDeadline(time.Now().Add(requestTimeout))
		if err := common.WritePacket(p.connection, common.PRODUCER_MESSAGE, p.Namespace, payload); err != nil {
			continue
		}
		packetType, _, respPayload, err := common.ReadPacket(p.connection)
		if err != nil {
			continue
		}
		p.connection.SetDeadline(time.Time{})
		if packetType == common.SERVER_ERROR && string(respPayload) == errNotLeaderPayload {
			continue
		}
		return
	}
}

/* Sends a disconnect signal to the broker and terminates the connection */
func (p *Producer) Disconnect() error {
	err := common.WritePacket(p.connection, common.DISCONNECT, p.Namespace, []byte(""))
	if err != nil {
		return fmt.Errorf("failed to send message to broker at namespace %s: %w", p.Namespace, err)
	}
	p.connection.Close()
	return nil
}

func (p *Producer) Produce(payload []byte) {
	p.producerChannel <- payload
}
