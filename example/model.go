package model

import (
	synapse "synapse/src/common"
	"time"
)

type ModelResponse struct {
	ResponseType synapse.TaskStatus
	Response     string
}

type SupervisorModelResponse struct {
	Namespace string
	Prompt    []byte
}

func Query(prompt string, context string) ModelResponse {
	time.Sleep(5 * time.Second)
	return ModelResponse{
		ResponseType: synapse.COMPLETED,
		Response:     prompt + " - " + context,
	}
}

func SupervisorQuery(prompt []byte) []SupervisorModelResponse {
	return []SupervisorModelResponse{
		{
			Namespace: "attractions",
			Prompt:    []byte("Find attractions in Singapore for under than $5000"),
		},
	}
}

// implement supervisor agent
// try running
// implement flights agent
// try running
// add mocks
// add user input/output mock
// add other 2 agents
