package synapse

import (
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

type Server struct {
	Port                  int
	Brokers               map[string]*Broker
	brokerFilepath        string
	brokerWriteBufferSize int
	logger                *zap.Logger
	mutex                 sync.Mutex
	raftNode              *raft.Raft
}

func NewServer(port int, filepath string, bufferSize int, log *zap.Logger) *Server {
	return &Server{
		Port:                  port,
		Brokers:               make(map[string]*Broker),
		brokerFilepath:        filepath,
		brokerWriteBufferSize: bufferSize,
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

func (s *Server) applyRaftCommand(cmd Command) error {
	future := s.raftNode.Apply(cmd.encode(), 5*time.Second)
	return future.Error()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		packetType, namespace, data, err := common.ReadPacket(conn)
		if err != nil {
			s.logger.Error("failed to read packet", zap.Error(err))
			continue
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
			response, err := broker.PeekOne()
			if err != nil || response == nil {
				common.WritePacket(conn, common.SERVER_ERROR, namespace, []byte("ERR_READ_FAILED_OR_EMPTY"))
				continue
			}

			err = common.WritePacket(conn, common.SERVER_MESSAGE, namespace, response)
			if err != nil {
				continue // failed to write to client so don't advance index
			}

			// replicate the read index advancement across cluster
			err = s.applyRaftCommand(Command{Type: CmdConsume, Namespace: namespace})
			if err != nil {
				s.logger.Error("failed to replicate read advance", zap.Error(err))
			}
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
