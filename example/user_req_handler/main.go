package main

import (
	"fmt"
	model "synapse/example"
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
	p.Produce([]byte(model.UserReq_singapore))
	time.Sleep(5 * time.Second)
	p.Disconnect()
}
