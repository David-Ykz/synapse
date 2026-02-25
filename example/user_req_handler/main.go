package main

import (
	"fmt"
	"synapse/example/constants"
	"synapse/src/client"
	"time"
)

func main() {
	time.Sleep(1 * time.Second)
	config := constants.NewUserReqToTaskAllocatorConfig()
	p := client.NewProducer(config)
	err := p.Connect()
	if err != nil {
		fmt.Println("user_req_handler.go:", err)
	}
	msg := "Plan me a trip to Singapore for 2 weeks during April with a budget of $5000"
	p.Produce([]byte(msg))
	time.Sleep(5 * time.Second)
	p.Disconnect()
}
