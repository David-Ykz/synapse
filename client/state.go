package synapse

import (
	"errors"
	"fmt"
	"net"
	"time"

	common "synapse/common"
)

// ErrNotFound is returned by Get when the key has no stored value
var ErrNotFound = errors.New("state: key not found")

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
	conn, err := dial(s.Host, s.Port)
	if err != nil {
		return err
	}
	s.connection = conn
	return nil
}

// reconnect closes the current connection and opens a fresh one to the same address; used to retry
// against a different broker pod after ERR_NOT_LEADER, since a k8s Service load-balances new
// connections across replicas but the wire protocol itself has no leader-redirect of its own
func (s *StateClient) reconnect() error {
	if s.connection != nil {
		s.connection.Close()
	}
	conn, err := dial(s.Host, s.Port)
	if err != nil {
		return err
	}
	s.connection = conn
	return nil
}

// request performs one request/response round trip, and reconnects/retries if the
// connected node isn't the raft leader or the connection drops mid-request
func (s *StateClient) request(packetType common.PacketType, key string, payload []byte) (common.PacketType, []byte, error) {
	var lastErr error
	for attempt := 0; attempt <= maxLeaderRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(leaderRetryDelay)
			if err := s.reconnect(); err != nil {
				lastErr = err
				continue
			}
		}
		s.connection.SetDeadline(time.Now().Add(requestTimeout))
		if err := common.WritePacket(s.connection, packetType, key, payload); err != nil {
			lastErr = fmt.Errorf("failed to write state request: %w", err)
			continue
		}
		respType, _, respPayload, err := common.ReadPacket(s.connection)
		if err != nil {
			lastErr = fmt.Errorf("failed to read state response: %w", err)
			continue
		}
		if respType == common.SERVER_ERROR && string(respPayload) == errNotLeaderPayload {
			lastErr = ErrNotLeader
			continue
		}
		return respType, respPayload, nil
	}
	return 0, nil, lastErr
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
	packetType, payload, err := s.request(common.STATE_SET, key, value)
	if err != nil {
		return err
	}
	if packetType == common.SERVER_ERROR {
		return stateError(payload)
	}
	return nil
}

// Get retrieves the value stored under key, returning ErrNotFound if it doesn't exist
func (s *StateClient) Get(key string) ([]byte, error) {
	packetType, payload, err := s.request(common.STATE_GET, key, []byte(""))
	if err != nil {
		return nil, err
	}
	if packetType == common.SERVER_ERROR {
		return nil, stateError(payload)
	}
	return payload, nil
}

// Delete removes key, it is idempotent and does not error on a missing key
func (s *StateClient) Delete(key string) error {
	packetType, payload, err := s.request(common.STATE_DELETE, key, []byte(""))
	if err != nil {
		return err
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
