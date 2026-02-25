package constants

import "synapse/src/common"

const (
	HandleUserRequest_TaskAllocator   = "HandleUserRequest_TaskAllocator"
	SUPERVISOR_USER_REQUEST_NAMESPACE = "Supervisor_UserRequest"
	SUPERVISOR_ATTRACTIONS_NAMESPACE  = "Supervisor_Attractions"
	ATTRACTIONS_SUPERVISOR_NAMESPACE  = "Attractions_Supervisor"
)

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
