package synapse

import (
	"fmt"
	"log"
	"net"
	"time"

	common "synapse/common"
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
			log.Println("Server.handleConnection() failed to read packet:", err)
			return
		}

		broker, exists := s.Brokers[namespace]
		if !exists {
			log.Printf("No broker found, creating broker in namespace %s with filepath %s\n", namespace, s.brokerFilepath)
			broker = NewBroker(0, namespace, s.brokerFilepath, s.brokerWriteBufferSize)
			err = broker.Initialize()
			if err != nil {
				log.Printf("Server.handleConnection() failed to initialize broker in namespace %s: %s\n", namespace, err)
			}
			s.Brokers[namespace] = broker
		}
		switch packetType {
		case common.DISCONNECT:
			log.Println("Client disconnected from namespace", namespace)
			return
		case common.PRODUCER_MESSAGE:
			log.Printf("Received message from producer in namespace %s: %s\n", namespace, data)
			err = broker.Write(data)
			if err != nil {
				log.Printf("Server.handleConnection() failed to write data to broker in namespace %s: %s\n", namespace, err)
				return
			}
		case common.CONSUMER_MESSAGE:
			log.Printf("Received request from consumer in namespace %s\n", namespace)
			response, err := broker.ReadOne()
			if err != nil {
				log.Printf("Server.handleConnection() failed to read from broker in namespace %s: %s\n", namespace, err)
				err = common.WritePacket(conn, common.SERVER_ERROR, namespace, []byte(""))
				if err != nil {
					log.Println("Server.handleConnection() failed to return a server error message:", err)
				}
				return
			}
			err = common.WritePacket(conn, common.SERVER_MESSAGE, namespace, response)
			if err != nil {
				log.Println("Server.handleConnection() failed to return a server response message:", err)
			}
		}
	}
}

func (s *Server) Start() error {
	var listener net.Listener
	var err error
	for i := 0; i < common.MAX_RETRIES; i++ {
		listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
		if err == nil {
			break
		}
		time.Sleep(common.RETRY_INTERVAL_RUNTIME * time.Second)
	}
	if err != nil {
		return err
	}
	log.Println("Broker listening on", s.Port)

	for {
		var conn net.Conn
		var err error
		for i := 0; i < common.MAX_RETRIES; i++ {
			conn, err = listener.Accept()
			if err == nil {
				break
			}
			time.Sleep(common.RETRY_INTERVAL_RUNTIME * time.Second)
		}
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go s.handleConnection(conn)
	}
}
