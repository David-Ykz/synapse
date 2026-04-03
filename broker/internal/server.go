package synapse

import (
	"fmt"
	"net"

	common "synapse/common"

	"go.uber.org/zap"
)

type Server struct {
	Port                  int
	Brokers               map[string]*Broker
	brokerFilepath        string
	brokerWriteBufferSize int
	logger                *zap.Logger
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

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		packetType, namespace, data, err := common.ReadPacket(conn)
		if err != nil {
			s.logger.Error("failed to read packet", zap.Error(err))
			continue
		}

		broker, exists := s.Brokers[namespace]
		if !exists {
			s.logger.Info("No broker found, creating broker",
				zap.String("namespace", namespace),
				zap.String("filepath", s.brokerFilepath),
			)
			broker = NewBroker(0, namespace, s.brokerFilepath, s.brokerWriteBufferSize)
			err = broker.Initialize()
			if err != nil {
				s.logger.Error("failed to initialize broker",
					zap.Error(err),
					zap.String("namespace", namespace),
				)
				continue
			}
			s.Brokers[namespace] = broker
		}
		switch packetType {
		case common.DISCONNECT:
			s.logger.Info("client disconnect",
				zap.String("namespace", namespace),
			)
			return
		case common.PRODUCER_MESSAGE:
			s.logger.Info("producer message",
				zap.String("namespace", namespace),
				zap.ByteString("data", data),
			)
			err = broker.Write(data)
			if err != nil {
				s.logger.Error("failed to write to broker",
					zap.String("namespace", namespace),
					zap.Error(err),
				)
			}
		case common.CONSUMER_MESSAGE:
			s.logger.Info("consumer read request",
				zap.String("namespace", namespace),
			)
			response, err := broker.ReadOne()
			if err != nil {
				s.logger.Error("failed to read from broker",
					zap.String("namespace", namespace),
					zap.Error(err),
				)
				err = common.WritePacket(conn, common.SERVER_ERROR, namespace, []byte(""))
				if err != nil {
					s.logger.Error("failed to return server error message",
						zap.String("namespace", namespace),
						zap.Error(err),
					)
				}
				continue
			}
			err = common.WritePacket(conn, common.SERVER_MESSAGE, namespace, response)
			if err != nil {
				s.logger.Error("failed to return server message",
					zap.String("namespace", namespace),
					zap.Error(err),
				)
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
