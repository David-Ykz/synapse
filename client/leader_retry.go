package synapse

import (
	"errors"
	"fmt"
	"net"
	"time"
)

// ErrNotLeader is returned when the connected broker node isn't the current raft leader
var ErrNotLeader = errors.New("client: not leader")

// maxLeaderRetries bounds how many times a client reconnects after ERR_NOT_LEADER before giving up;
// a Service load-balancing across N broker replicas means a fresh connection can land on any
// follower, so this needs to comfortably exceed a typical replica count
const maxLeaderRetries = 5

// leaderRetryDelay is the pause before each reconnect attempt, giving a raft election time to settle
const leaderRetryDelay = 200 * time.Millisecond

// errNotLeaderPayload is the SERVER_ERROR payload the broker sends when the connected node isn't leader
const errNotLeaderPayload = "ERR_NOT_LEADER"

// requestTimeout bounds how long a single connect, or a single write+read round trip, can take,
// so a retry loop can't get stuck hanging on one bad attempt instead of moving on to the next
const requestTimeout = 5 * time.Second

// dial opens a fresh TCP connection to host:port
func dial(host string, port int) (net.Conn, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, requestTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker at address %s: %w", addr, err)
	}
	return conn, nil
}
