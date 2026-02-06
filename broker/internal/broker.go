package broker

import (
	"fmt"
	"net"
	"synapse/common"
)

type Server struct {
	Port int
}

func NewServer(port int) *Server {
	return &Server{Port: port}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		packetType, namespace, data, err := common.ReadPacket(conn)
		if err != nil {
			fmt.Println("Error in reading packet:", err)
			return
		}
		switch packetType {
		case common.DISCONNECT:
			return
		case common.PRODUCER_MESSAGE:
			fmt.Printf("Recived message from producer in namespace %s: %s\n", namespace, data)
		case common.CONSUMER_MESSAGE:
			if namespace == "namespace1" {
				common.WritePacket(conn, common.SERVER_MESSAGE, namespace, "World Hello")
			} else {
				common.WritePacket(conn, common.SERVER_MESSAGE, namespace, "")
			}
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
