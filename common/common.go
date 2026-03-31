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
)

type TaskStatus byte

const (
	CREATED TaskStatus = iota
	CONSUMED
	COMPLETED
	CONSTRAINT_VIOLATION
	ERROR
)

type Task struct {
	TaskId             string      `json:"task_id"`
	RequestId          string      `json:"request_id"`
	Namespace          string      `json:"namespace"`
	Status             TaskStatus  `json:"task_status"`
	Prompt             []byte      `json:"prompt"`
	Response           interface{} `json:"response"`
	CreatedTimestamp   int64       `json:"created_timestamp"`
	CompletedTimestamp int64       `json:"completed_timestamp"`
}

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
		err = fmt.Errorf("ReadPacket() failed to read header: %w", err)
		return
	}
	packetType = PacketType(header[2])
	namespaceLength := header[3]
	payloadLength := binary.BigEndian.Uint32(header[4:8])

	namespaceBuffer := make([]byte, namespaceLength)
	_, err = io.ReadFull(conn, namespaceBuffer)
	if err != nil {
		err = fmt.Errorf("ReadPacket() failed to read namespace: %w", err)
		return
	}
	namespace = string(namespaceBuffer)

	if payloadLength == 0 {
		return
	}
	payload = make([]byte, payloadLength)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		err = fmt.Errorf("ReadPacket() failed to read payload: %w", err)
	}

	return
}

func WritePacket(conn net.Conn, packetType PacketType, namespace string, payload []byte) error {
	header := make([]byte, 8)
	header[2] = byte(packetType)
	header[3] = byte(len(namespace))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))

	_, err := conn.Write(header)
	if err != nil {
		return fmt.Errorf("WritePacket() failed to write header: %w", err)
	}

	_, err = conn.Write([]byte(namespace))
	if err != nil {
		return fmt.Errorf("WritePacket() failed to write: %w", err)
	}

	_, err = conn.Write(payload)
	if err != nil {
		return fmt.Errorf("WritePacket() failed to write payload: %w", err)
	}

	return nil
}
