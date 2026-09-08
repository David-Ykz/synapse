package synapse

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type PacketType byte

const (
	DISCONNECT PacketType = iota
	PRODUCER_MESSAGE
	CONSUMER_MESSAGE
	SERVER_ERROR
	SERVER_MESSAGE
	CONSUMER_ACK
	CONSUMER_NACK
	STATE_SET
	STATE_GET
	STATE_DELETE
)

type Config struct {
	Host        string
	Port        int
	Namespace   string
	ChannelSize int
}

func ReadPacket(conn net.Conn) (packetType PacketType, namespace string, payload []byte, err error) {
	header := make([]byte, 8)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		err = fmt.Errorf("failed to read header: %w", err)
		return
	}
	packetType = PacketType(header[2])
	namespaceLength := header[3]
	payloadLength := binary.BigEndian.Uint32(header[4:8])

	namespaceBuffer := make([]byte, namespaceLength)
	_, err = io.ReadFull(conn, namespaceBuffer)
	if err != nil {
		err = fmt.Errorf("failed to read namespace: %w", err)
		return
	}
	namespace = string(namespaceBuffer)

	if payloadLength == 0 {
		return
	}
	payload = make([]byte, payloadLength)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		err = fmt.Errorf("failed to read payload: %w", err)
	}

	return
}

func WritePacket(conn net.Conn, packetType PacketType, namespace string, payload []byte) error {
	if len(namespace) > 255 {
		return fmt.Errorf("namespace or key too long: %d bytes, max 255", len(namespace))
	}

	header := make([]byte, 8)
	header[2] = byte(packetType)
	header[3] = byte(len(namespace))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))

	_, err := conn.Write(header)
	if err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	_, err = conn.Write([]byte(namespace))
	if err != nil {
		return fmt.Errorf("failed to write namespace: %w", err)
	}

	_, err = conn.Write(payload)
	if err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	return nil
}
