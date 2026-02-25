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
	c := client.NewConsumer(client.ConsumerConfig{
		Config:         constants.NewTaskAllocatorToAttractionsAgentConfig(),
		PollIntervalMs: 1000,
		PollBackoff:    false,
	})
	c.Connect()
	fmt.Println("oog1a")
	input := c.Subscribe()

	fmt.Println("ooga2")
	p := client.NewProducer(constants.NewAttractionsAgentToTaskCombinerConfig())
	p.Connect()

	fmt.Println("ooga")

	for event := range input {
		if event.Error != nil {
			fmt.Println("attractions_agent.go:", event.Error)
		} else {
			fmt.Println("attractions_agent.go:", string(event.Payload))
			response := model.Query(event.Payload, []byte(""))
			fmt.Println(string(response))
		}
	}

	c.Disconnect()
	p.Disconnect()
}
