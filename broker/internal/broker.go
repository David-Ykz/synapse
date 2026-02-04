package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Server struct {
	port int
}

func NewServer(port int) *Server {
	return &Server{port: port}
}

func readPacket(conn net.Conn) (clientType byte, namespace string, payload []byte, err error) {
	header := make([]byte, 8)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return 0, "", nil, err
	}
	clientType = header[2]
	namespaceLength := header[3]
	payloadLength := binary.BigEndian.Uint32(header[4:8])

	namespaceBuffer := make([]byte, namespaceLength)
	_, err = io.ReadFull(conn, namespaceBuffer)
	if err != nil {
		return
	}
	namespace = string(namespaceBuffer)

	payload = make([]byte, payloadLength)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return
	}

	return
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		clientType, namespace, data, err := readPacket(conn)
		if err != nil {
			fmt.Println("Error in reading packet", err)
			return
		}
		fmt.Printf("Recived %x message in namespace %s: %s\n", clientType, namespace, data)
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}
	fmt.Println("Broker listening on", s.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection", err)
			continue
		}
		go s.handleConnection(conn)
	}
}
