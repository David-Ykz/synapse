package synapse

import (
	"errors"
	"fmt"
	"net"

	common "synapse/common"
)

// ErrNotFound is returned by Get when the key has no stored value
var ErrNotFound = errors.New("state: key not found")

// ErrNotLeader is returned when the connected broker node isn't the current raft leader; callers may want to retry against a different node
var ErrNotLeader = errors.New("state: not leader")

// ErrCapacityExceeded is returned by Set when the store has no room left for the new value
var ErrCapacityExceeded = errors.New("state: capacity exceeded")

// ErrValueTooLarge is returned by Set when the value exceeds the store's per-value size limit
var ErrValueTooLarge = errors.New("state: value too large")

// StateClient is a synchronous request/response client for the broker's key-value state store, unlike Producer/Consumer this has no background goroutine: each call writes one request and blocks for exactly one response
type StateClient struct {
	Host       string
	Port       int
	connection net.Conn
}

// NewStateClient initializes a new StateClient
func NewStateClient(host string, port int) *StateClient {
	return &StateClient{Host: host, Port: port}
}

// Connect connects to the broker
func (s *StateClient) Connect() error {
	addr := net.JoinHostPort(s.Host, fmt.Sprintf("%d", s.Port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to broker at address %s: %w", addr, err)
	}
	s.connection = conn
	return nil
}

// Disconnect sends a disconnect signal to the broker and terminates the connection
func (s *StateClient) Disconnect() error {
	err := common.WritePacket(s.connection, common.DISCONNECT, "", []byte(""))
	if err != nil {
		return fmt.Errorf("failed to send disconnect to broker: %w", err)
	}
	s.connection.Close()
	return nil
}

// Set stores value under key
func (s *StateClient) Set(key string, value []byte) error {
	if err := common.WritePacket(s.connection, common.STATE_SET, key, value); err != nil {
		return fmt.Errorf("failed to write state set request: %w", err)
	}
	packetType, _, payload, err := common.ReadPacket(s.connection)
	if err != nil {
		return fmt.Errorf("failed to read state set response: %w", err)
	}
	if packetType == common.SERVER_ERROR {
		return stateError(payload)
	}
	return nil
}

// Get retrieves the value stored under key, returning ErrNotFound if it doesn't exist
func (s *StateClient) Get(key string) ([]byte, error) {
	if err := common.WritePacket(s.connection, common.STATE_GET, key, []byte("")); err != nil {
		return nil, fmt.Errorf("failed to write state get request: %w", err)
	}
	packetType, _, payload, err := common.ReadPacket(s.connection)
	if err != nil {
		return nil, fmt.Errorf("failed to read state get response: %w", err)
	}
	if packetType == common.SERVER_ERROR {
		return nil, stateError(payload)
	}
	return payload, nil
}

// Delete removes key, it is idempotent and does not error on a missing key
func (s *StateClient) Delete(key string) error {
	if err := common.WritePacket(s.connection, common.STATE_DELETE, key, []byte("")); err != nil {
		return fmt.Errorf("failed to write state delete request: %w", err)
	}
	packetType, _, payload, err := common.ReadPacket(s.connection)
	if err != nil {
		return fmt.Errorf("failed to read state delete response: %w", err)
	}
	if packetType == common.SERVER_ERROR {
		return stateError(payload)
	}
	return nil
}

// stateError maps a SERVER_ERROR payload to a sentinel error where one exists
func stateError(payload []byte) error {
	switch string(payload) {
	case "ERR_NOT_FOUND":
		return ErrNotFound
	case "ERR_NOT_LEADER":
		return ErrNotLeader
	case "ERR_CAPACITY_EXCEEDED":
		return ErrCapacityExceeded
	case "ERR_VALUE_TOO_LARGE":
		return ErrValueTooLarge
	default:
		return fmt.Errorf("state: server error: %s", payload)
	}
}
