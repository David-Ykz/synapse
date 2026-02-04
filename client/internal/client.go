package client

import (
	"encoding/binary"
	"fmt"
	"net"
)

type Config struct {
	Host       string
	Port       int
	ClientType byte
	Namespace  string
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

func (p *Client) Send(payload string) error {
	header := make([]byte, 8)
	header[2] = p.config.ClientType
	header[3] = byte(len(p.config.Namespace))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	packet := append(header, []byte(p.config.Namespace)...)
	packet = append(packet, []byte(payload)...)

	_, err := p.connection.Write(packet)
	if err != nil {
		return fmt.Errorf("error sending message to broker: %w", err)
	}
	return nil
}
