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
	Prompts []string
	Context string
}

func Query(prompt string, context string) ModelResponse {
	time.Sleep(5 * time.Second)
	return ModelResponse{
		ResponseType: synapse.COMPLETED,
		Response:     prompt + " - " + context,
	}
}

func SupervisorQuery(prompt string, context string) SupervisorModelResponse {
	time.Sleep(5 * time.Second)
	return SupervisorModelResponse{
		Prompts: []string{
			"Find attractions in Singapore",
		},
		Context: "Budget is $5000, timeframe is in April",
	}
}

// implement supervisor agent
// try running
// implement flights agent
// try running
// add mocks
// add user input/output mock
// add other 2 agents
