package main

import (
	model "synapse/example"
	synapse "synapse/src/client"
	common "synapse/src/common"
	"time"

	"github.com/google/uuid"
)

func onUserRequest(s *synapse.SupervisorClient, prompt []byte) error {
	requestId := uuid.New().String()
	response := model.SupervisorQuery(string(prompt), "")
	for _, prompt := range response.Prompts {
		s.AddTask(requestId, prompt, response.Context)
	}
	return nil
}

func onTaskEvent(s *synapse.SupervisorClient, task common.Task) error {
	return nil
}

func main() {
	attractionsConfig := synapse.Config{
		Host:              "localhost",
		Port:              8080,
		DefaultNamespace:  "attractions",
		PollIntervalMs:    1000,
		MaxPollIntervalMs: 60000,
		PollBackoff:       true,
	}
	configs := []synapse.Config{attractionsConfig}

	client := synapse.NewSupervisorClient(configs, onUserRequest, onTaskEvent)
	client.Start()

	time.Sleep(1 * time.Minute)
	client.Stop()
}
