package main

import (
	"fmt"
	broker "synapse/broker/internal"
)

func main() {
	server := broker.NewServer(8080)
	err := server.Start()
	if err != nil {
		fmt.Println("Server exited with error", err)
	}
}
