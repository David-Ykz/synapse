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
		Config:         constants.NewUserReqToTaskAllocatorConfig(),
		PollIntervalMs: 1000,
		PollBackoff:    false,
	})
	c.Connect()
	input := c.Subscribe()

	p := client.NewProducer(constants.NewTaskAllocatorToAttractionsAgentConfig())
	p.Connect()

	for event := range input {
		if event.Error != nil {
			fmt.Println("task_allocator.go:", event.Error)
		} else {
			fmt.Println("task_allocator.go:", string(event.Payload))
			tasks := model.DistributeTasks(event.Payload)
			for _, task := range tasks {
				if task.Namespace == "TaskAllocator_AttractionsAgent" {
					p.Produce(task.Prompt)
				}
			}
		}
	}

	c.Disconnect()
	p.Disconnect()
}
