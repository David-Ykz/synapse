package client

import (
	"context"
	"encoding/json"
	"fmt"
	"synapse/src/common"
	"sync"
	"time"

	"github.com/google/uuid"
)

type UserRequestHandler func(s *SupervisorClient, prompt []byte) error
type TaskEventHandler func(s *SupervisorClient, task common.Task) error

type SupervisorClient struct {
	clients       map[string]*BasicClient
	taskList      map[string]*common.Task
	onUserRequest UserRequestHandler
	onTaskEvent   TaskEventHandler
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func NewSupervisorClient(configs []Config, onUserRequest UserRequestHandler, onTaskEvent TaskEventHandler) *SupervisorClient {
	clients := make(map[string]*BasicClient)
	for _, config := range configs {
		clients[config.DefaultNamespace] = NewBasicClient(config)
	}

	return &SupervisorClient{
		clients:       clients,
		onUserRequest: onUserRequest,
		onTaskEvent:   onTaskEvent,
	}
}

func (s *SupervisorClient) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	for namespace, client := range s.clients {
		err := client.Connect()
		if err != nil {
			return fmt.Errorf("SupervisorClient.Initialize() failed to connect to broker in namespace %s: %w", namespace, err)
		}
		s.wg.Add(1)
		go s.HandleConnection(ctx, namespace)
	}
	fmt.Println("Successfully started supervisor")
	return nil
}

func (s *SupervisorClient) HandleConnection(ctx context.Context, namespace string) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			payload, err := s.clients[namespace].Consume(namespace)
			if err != nil {
				fmt.Printf("SupervisorClient.HandleConnection() failed to consume event in namespace %s: %w\n", namespace, err)
				continue
			}
			if namespace == "user_request" {
				err = s.onUserRequest(s, payload)
				if err != nil {
					fmt.Printf("SupervisorClient.HandleConnection() failed to handle user request in namespace %s: %w\n", err)
				}
			} else {
				var task common.Task
				err = json.Unmarshal(payload, &task)
				if err != nil {
					fmt.Printf("SupervisorClient.HandleConnection() failed to parse json in namespace %s: %w\n", err)
					continue
				}
				err = s.onTaskEvent(s, task)
				if err != nil {
					fmt.Printf("SupervisorClient.HandleConnection() failed to handle task in namespace %s: %w\n", err)
				}
			}
		}
	}
}

func (s *SupervisorClient) Stop() (err error) {
	if s.cancel != nil {
		s.cancel()
	}

	s.wg.Wait()

	for namespace, client := range s.clients {
		err = client.Disconnect(namespace)
		if err != nil {
			fmt.Printf("SupervisorClient.Stop() failed to send disconnect to broker in namespace %s: %w\n", namespace, err)
		}
	}

	if err == nil {
		fmt.Println("Successfully stopped supervisor")
	} else {
		fmt.Println("Supervisor stopped with some errors")
	}
	return
}

func (s *SupervisorClient) AddTask(requestId string, prompt string, context string) *common.Task {
	task := common.Task{
		RequestId:        requestId,
		TaskId:           uuid.New().String(),
		Status:           common.CREATED,
		Prompt:           prompt,
		Context:          context,
		CreatedTimestamp: time.Now().Unix(),
	}
	s.taskList[task.TaskId] = &task
	return &task
}

func (s *SupervisorClient) RemoveTask(taskId string) *common.Task {
	task, exists := s.taskList[taskId]
	if !exists {
		return nil
	}
	return task
}
