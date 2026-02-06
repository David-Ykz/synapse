package common

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
	SERVER_MESSAGE
)

func ReadPacket(conn net.Conn) (packetType PacketType, namespace string, payload string, err error) {
	header := make([]byte, 8)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		err = fmt.Errorf("Error reading header: %w", err)
		return
	}
	packetType = PacketType(header[2])
	namespaceLength := header[3]
	payloadLength := binary.BigEndian.Uint32(header[4:8])

	namespaceBuffer := make([]byte, namespaceLength)
	_, err = io.ReadFull(conn, namespaceBuffer)
	if err != nil {
		err = fmt.Errorf("Error reading namespace: %w", err)
		return
	}
	namespace = string(namespaceBuffer)

	if payloadLength == 0 {
		return
	}
	payloadBytes := make([]byte, payloadLength)
	_, err = io.ReadFull(conn, payloadBytes)
	if err != nil {
		err = fmt.Errorf("Error reading payload: %w", err)
		return
	}
	payload = string(payloadBytes)

	return
}

func WritePacket(conn net.Conn, packetType PacketType, namespace string, payload string) error {
	header := make([]byte, 8)
	header[2] = byte(packetType)
	header[3] = byte(len(namespace))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))

	if _, err := conn.Write(header); err != nil {
		err = fmt.Errorf("Error writing header: %w", err)
		return err
	}
	if _, err := conn.Write([]byte(namespace)); err != nil {
		err = fmt.Errorf("Error writing namespace: %w", err)
		return err
	}
	if _, err := conn.Write([]byte(payload)); err != nil {
		err = fmt.Errorf("Error writing payload: %w", err)
		return err
	}

	return nil
}
