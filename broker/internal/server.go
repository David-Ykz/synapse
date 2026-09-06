package synapse

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	common "synapse/common"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

type LagSnapshot struct {
	Namespace string `json:"namespace"`
	Lag       int64  `json:"lag"`
}

type Server struct {
	Port                  int
	Brokers               map[string]*Broker
	brokerFilepath        string
	brokerWriteBufferSize int
	logger                *zap.Logger
	mutex                 sync.Mutex
	raftNode              *raft.Raft
	maxRetries            int
	backoffBase           time.Duration
	backoffMax            time.Duration
}

func NewServer(port int, filepath string, bufferSize int, maxRetries int, backoffBase, backoffMax time.Duration, log *zap.Logger) *Server {
	return &Server{
		Port:                  port,
		Brokers:               make(map[string]*Broker),
		brokerFilepath:        filepath,
		brokerWriteBufferSize: bufferSize,
		maxRetries:            maxRetries,
		backoffBase:           backoffBase,
		backoffMax:            backoffMax,
		logger:                log,
	}
}

func (s *Server) getOrCreateBroker(namespace string) *Broker {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	broker, exists := s.Brokers[namespace]
	if !exists {
		broker = NewBroker(0, namespace, s.brokerFilepath, s.brokerWriteBufferSize)
		broker.Initialize()
		s.Brokers[namespace] = broker
	}
	return broker
}

func (s *Server) SetupRaft(serverID, advertiseAddr, bindAddr string, bootstrapPeers map[string]string) error {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(serverID)

	raftDir := filepath.Join(s.brokerFilepath, "raft")
	os.MkdirAll(raftDir, 0755)

	// setup Raft stores
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "logs.dat"))
	if err != nil {
		return err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.dat"))
	if err != nil {
		return err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 2, os.Stdout)
	if err != nil {
		return err
	}

	// bind to 0.0.0.0 but advertise the pod's DNS name
	advAddr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(bindAddr, advAddr, 3, 10*time.Second, os.Stdout)
	if err != nil {
		return err
	}

	fsm := &brokerFSM{server: s}
	s.raftNode, err = raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	// bootstrap cluster
	if len(bootstrapPeers) > 0 {
		var configuration raft.Configuration
		for id, addr := range bootstrapPeers {
			configuration.Servers = append(configuration.Servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: raft.ServerAddress(addr),
			})
		}
		s.raftNode.BootstrapCluster(configuration)
	}

	return nil
}

func (s *Server) GetLagSnapshot() []LagSnapshot {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	out := make([]LagSnapshot, 0, len(s.Brokers))
	for ns, b := range s.Brokers {
		out = append(out, LagSnapshot{Namespace: ns, Lag: b.Lag()})
	}
	return out
}

func (s *Server) applyRaftCommand(cmd Command) error {
	future := s.raftNode.Apply(cmd.encode(), 5*time.Second)
	return future.Error()
}

func encodeIndex(messageIndex int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(messageIndex))
	return buf
}

// ack replicates that messageIndex has been resolved for a namespace
func (s *Server) ack(namespace string, messageIndex int64) {
	if err := s.applyRaftCommand(Command{Type: CmdConsume, Namespace: namespace, Data: encodeIndex(messageIndex)}); err != nil {
		s.logger.Error("failed to replicate ack", zap.String("namespace", namespace), zap.Error(err))
	}
}

// moveToDLQ moves a message that has exhausted all retries to its respective DLQ
func (s *Server) moveToDLQ(namespace string, messageIndex int64, payload []byte) {
	dlqNamespace := namespace + DLQNamespaceSuffix
	if err := s.applyRaftCommand(Command{Type: CmdProduce, Namespace: dlqNamespace, Data: payload}); err != nil {
		s.logger.Error("failed to write dead letter", zap.String("namespace", dlqNamespace), zap.Error(err))
	}
	s.ack(namespace, messageIndex)
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	var pendingBroker *Broker
	pendingIndex := int64(-1)

	releasePending := func() {
		if pendingBroker != nil {
			// treat an abandoned connection as an explicit NACK, so the message doesn't just sit out its full visibility timeout
			pendingBroker.Nack(pendingIndex)
			pendingBroker, pendingIndex = nil, -1
		}
	}
	defer releasePending()

	for {
		packetType, namespace, data, err := common.ReadPacket(conn)
		if err != nil {
			s.logger.Error("failed to read packet", zap.Error(err))
			return
		}

		if s.raftNode.State() != raft.Leader {
			s.logger.Warn("Rejecting request, not leader")
			common.WritePacket(conn, common.SERVER_ERROR, namespace, []byte("ERR_NOT_LEADER"))
			return
		}

		broker := s.getOrCreateBroker(namespace)

		switch packetType {
		case common.DISCONNECT:
			return

		case common.PRODUCER_MESSAGE:
			// apply write via Raft
			err := s.applyRaftCommand(Command{Type: CmdProduce, Namespace: namespace, Data: data})
			if err != nil {
				s.logger.Error("failed to replicate write", zap.Error(err))
			}

		case common.CONSUMER_MESSAGE:
			var messageIndex int64 = -1
			var payload []byte
			var deliverErr error
			for {
				var dlIndex int64
				var dlPayload []byte
				messageIndex, payload, dlIndex, dlPayload, deliverErr = broker.Deliver(s.maxRetries, s.backoffBase, s.backoffMax)
				if deliverErr != nil {
					break
				}
				if dlIndex != -1 {
					s.moveToDLQ(namespace, dlIndex, dlPayload)
					continue // this poll may still find a real message (or another dead letter) now
				}
				break
			}

			if deliverErr != nil {
				s.logger.Error("failed to deliver message", zap.Error(deliverErr))
				common.WritePacket(conn, common.SERVER_ERROR, namespace, []byte("ERR_READ_FAILED"))
				continue
			}
			if payload == nil {
				common.WritePacket(conn, common.SERVER_ERROR, namespace, []byte("ERR_EMPTY"))
				continue
			}

			if err := common.WritePacket(conn, common.SERVER_MESSAGE, namespace, payload); err != nil {
				broker.Nack(messageIndex) // client never actually received it; make it redeliverable right away
				return
			}
			pendingBroker, pendingIndex = broker, messageIndex

		case common.CONSUMER_ACK:
			if pendingBroker != nil {
				s.ack(namespace, pendingIndex)
			}
			pendingBroker, pendingIndex = nil, -1

		case common.CONSUMER_NACK:
			if pendingBroker != nil {
				pendingBroker.Nack(pendingIndex)
			}
			pendingBroker, pendingIndex = nil, -1
		}
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return err
	}
	s.logger.Info("started Broker service", zap.Int("port", s.Port))

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.logger.Error("failed to accept connection", zap.Error(err))
			continue
		}
		go s.handleConnection(conn)
	}
}
