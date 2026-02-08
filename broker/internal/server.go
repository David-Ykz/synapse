package broker

import (
	"fmt"
	"net"
	"synapse/common"
)

type Server struct {
	Port                  int
	Brokers               map[string]*Broker
	brokerFilepath        string
	brokerWriteBufferSize int
}

func NewServer(port int, filepath string, bufferSize int) *Server {
	return &Server{
		Port:                  port,
		Brokers:               make(map[string]*Broker),
		brokerFilepath:        filepath,
		brokerWriteBufferSize: bufferSize,
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		packetType, namespace, data, err := common.ReadPacket(conn)
		if err != nil {
			fmt.Println("Error in reading packet:", err)
			return
		}
		broker, exists := s.Brokers[namespace]

		if !exists {
			broker = NewBroker(0, namespace, s.brokerFilepath, s.brokerWriteBufferSize)
			broker.Initialize()
			s.Brokers[namespace] = broker
		}
		switch packetType {
		case common.DISCONNECT:
			return
		case common.PRODUCER_MESSAGE:
			fmt.Printf("Recived message from producer in namespace %s: %s\n", namespace, data)
			err = broker.Write(data)
			if err != nil {
				fmt.Println("Error in writing to broker:", err)
				return
			}
		case common.CONSUMER_MESSAGE:
			response, err := broker.ReadOne()
			if err != nil {
				fmt.Printf("Error reading from broker in namespace %s: %s\n", namespace, err)
				common.WritePacket(conn, common.SERVER_ERROR, namespace, "")
				return
			}
			common.WritePacket(conn, common.SERVER_MESSAGE, namespace, string(response))
		}
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return err
	}
	fmt.Println("Broker listening on", s.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection", err)
			continue
		}
		go s.handleConnection(conn)
	}
}
