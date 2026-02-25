package broker

import (
	"fmt"
	"net"
	common "synapse/src/common"
)

type Server struct {
	Port                  int
	Brokers               map[string]*Broker
	brokerFilepath        string
	brokerWriteBufferSize int
	debug                 bool
}

func NewServer(port int, filepath string, bufferSize int, debug bool) *Server {
	return &Server{
		Port:                  port,
		Brokers:               make(map[string]*Broker),
		brokerFilepath:        filepath,
		brokerWriteBufferSize: bufferSize,
		debug:                 debug,
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		packetType, namespace, data, err := common.ReadPacket(conn)
		if err != nil {
			fmt.Println("Server.handleConnection() failed to read packet:", err)
			return
		}

		broker, exists := s.Brokers[namespace]
		if !exists {
			if s.debug {
				fmt.Printf("No broker found, creating broker in namespace %s with filepath %s\n", namespace, s.brokerFilepath)
			}
			broker = NewBroker(0, namespace, s.brokerFilepath, s.brokerWriteBufferSize)
			err = broker.Initialize()
			if err != nil {
				fmt.Printf("Server.handleConnection() failed to initialize broker in namespace %s: %s\n", namespace, err)
			}
			s.Brokers[namespace] = broker
		}
		switch packetType {
		case common.DISCONNECT:
			if s.debug {
				fmt.Println("Client disconnected from namespace", namespace)
			}
			return
		case common.PRODUCER_MESSAGE:
			if s.debug {
				fmt.Printf("Received message from producer in namespace %s: %s\n", namespace, data)
			}
			err = broker.Write(data)
			if err != nil {
				fmt.Printf("Server.handleConnection() failed to write data to broker in namespace %s: %s\n", namespace, err)
				return
			}
		case common.CONSUMER_MESSAGE:
			if s.debug {
				fmt.Printf("Received request from consumer in namespace %s\n", namespace)
			}
			response, err := broker.ReadOne()
			if err != nil {
				fmt.Printf("Server.handleConnection() failed to read from broker in namespace %s: %s\n", namespace, err)
				err = common.WritePacket(conn, common.SERVER_ERROR, namespace, []byte(""))
				if err != nil {
					fmt.Println("Server.handleConnection() failed to return a server error message:", err)
				}
				return
			}
			err = common.WritePacket(conn, common.SERVER_MESSAGE, namespace, response)
			if err != nil {
				fmt.Println("Server.handleConnection() failed to return a server response message:", err)
			}
		}
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return err
	}
	if s.debug {
		fmt.Println("Broker listening on", s.Port)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go s.handleConnection(conn)
	}
}
