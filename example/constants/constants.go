package constants

import "synapse/src/common"

var (
	BaseConfig = common.Config{
		Host:        "localhost",
		Port:        8080,
		ChannelSize: 64,
	}
)

func NewUserReqToTaskAllocatorConfig() common.Config {
	config := BaseConfig
	config.Namespace = "HandleUserRequest_TaskAllocator"
	return config
}

func NewTaskAllocatorToAttractionsAgentConfig() common.Config {
	config := BaseConfig
	config.Namespace = "TaskAllocator_AttractionsAgent"
	return config
}

func NewAttractionsAgentToTaskCombinerConfig() common.Config {
	config := BaseConfig
	config.Namespace = "AttractionsAgent_TaskCombiner"
	return config
}
