package client

import (
	"encoding/json"
	"fmt"
	"synapse/src/common"
)

type AgenticClient struct {
	basicClient *BasicClient
	Task        common.Task
}

func NewAgenticClient(config Config) *AgenticClient {
	b := NewBasicClient(config)
	return &AgenticClient{
		basicClient: b,
	}
}

func (a *AgenticClient) Connect() error {
	return a.basicClient.Connect()
}

func (a *AgenticClient) Disconnect() error {
	return a.basicClient.Disconnect(a.basicClient.config.DefaultNamespace)
}

func (a *AgenticClient) GetTask() error {
	payload, err := a.basicClient.Consume(a.basicClient.config.DefaultNamespace)
	if err != nil {
		return fmt.Errorf("AgenticClient.GetTask() failed to consume event: %w", err)
	}
	err = json.Unmarshal(payload, &a.Task)
	if err != nil {
		return fmt.Errorf("AgenticClient.GetTask() failed to parse json: %w", err)
	}
	return nil
}

func (a *AgenticClient) FinishTask(namespace string) error {
	payload, err := json.Marshal(a.Task)
	if err != nil {
		return fmt.Errorf("AgenticClient.FinishTask() failed to convert task to bytes: %w", err)
	}

	err = a.basicClient.Produce(a.basicClient.config.DefaultNamespace, payload)
	if err != nil {
		return fmt.Errorf("AgenticClient.FinishTask() failed to produce data to broker: %w", err)
	}
	return nil
}

func (a *AgenticClient) GetRelatedTasks() (tasks []common.Task, err error) {
	// call TaskList.getTasksByRequestId()
	return
}
