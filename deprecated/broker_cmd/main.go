package main

import (
	"fmt"
	broker "synapse/broker/internal"
)

func main() {
	server := broker.NewServer(8080, "/home/kzdavid/Github/synapse/temp", 4096)
	err := server.Start()
	if err != nil {
		fmt.Println("Server exited with error", err)
	}
}
