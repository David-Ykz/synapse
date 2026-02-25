package state

import "synapse/src/common"

type State struct {
	status  map[string]common.TaskStatus
	context []byte
}

var db map[string]*State

func AddState(requestId string, namespaces []string, context []byte) {
	state := State{
		status:  make(map[string]common.TaskStatus),
		context: context,
	}
	for _, namespace := range namespaces {
		state.status[namespace] = common.CREATED
	}
	db[requestId] = &state
}

func UpdateStateStatus(requestId string, namespace string, newStatus common.TaskStatus) {
	state := db[requestId]
	state.status[namespace] = newStatus
}

func UpdateStateContext(requestId string, context []byte, overwrite bool) {
	state := db[requestId]
	if overwrite {
		state.context = context
	} else {
		newContext := make([]byte, 0, len(context)+len(state.context))
		newContext = append(newContext, state.context...)
		newContext = append(newContext, context...)
		state.context = newContext
	}
}

func GetState(requestId string) *State {
	return db[requestId]
}

func DeleteState(requestId string) {
	delete(db, requestId)
}
