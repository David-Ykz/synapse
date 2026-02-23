package main

import (
	"fmt"
	model "synapse/example"
	synapse "synapse/src/client"
)

func main() {
	config := synapse.Config{
		Host:              "localhost",
		Port:              8080,
		DefaultNamespace:  "attractions",
		PollIntervalMs:    1000,
		MaxPollIntervalMs: 60000,
		PollBackoff:       true,
	}

	client := synapse.NewAgenticClient(config)
	client.Connect()

	for {
		err := client.GetTask()
		if err != nil {
			fmt.Println("Attractions agent encountered error", err)
		}
		prompt := client.Task.Prompt
		context := client.Task.Context
		modelResponse := model.Query(prompt, context)
		client.Task.Response = modelResponse.Response
		fmt.Printf("Response: %s", client.Task.Response)
		client.Task.Status = modelResponse.ResponseType
		client.FinishTask("")
	}

}
