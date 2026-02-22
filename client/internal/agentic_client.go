package client

import (
	"encoding/json"
	"fmt"
	"synapse/common"
)

type AgenticClient struct {
	basicClient      *BasicClient
	task             common.Task
	defaultNamespace string
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
	return a.basicClient.Disconnect(a.defaultNamespace)
}

func (a *AgenticClient) GetTask() error {
	payload, err := a.basicClient.Consume(a.defaultNamespace)
	if err != nil {
		return fmt.Errorf("AgenticClient.GetTask() failed to consume event: %w", err)
	}
	err = json.Unmarshal(payload, &a.task)
	if err != nil {
		return fmt.Errorf("AgenticClient.GetTask() failed to parse json: %w", err)
	}
	return nil
}

func (a *AgenticClient) FinishTask(namespace string) error {
	payload, err := json.Marshal(a.task)
	if err != nil {
		return fmt.Errorf("AgenticClient.FinishTask() failed to convert task to bytes: %w", err)
	}
	if namespace == "" {
		namespace = a.defaultNamespace
	}
	err = a.basicClient.Produce(namespace, payload)
	if err != nil {
		return fmt.Errorf("AgenticClient.FinishTask() failed to produce data to broker: %w", err)
	}
	return nil
}

func (a *AgenticClient) GetRelatedTasks() (tasks []common.Task, err error) {
	// call TaskList.getTasksByRequestId()
	return
}
