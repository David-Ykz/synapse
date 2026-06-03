package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	client "github.com/David-Ykz/synapse/client"
	common "github.com/David-Ykz/synapse/common"
	"github.com/David-Ykz/synapse/supervisor"
	models "github.com/David-Ykz/synapse/workers/models"

	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost == "" {
		brokerHost = "localhost"
	}
	brokerPort := 8080
	if p := os.Getenv("BROKER_PORT"); p != "" {
		brokerPort, _ = strconv.Atoi(p)
	}

	consumerConfig := client.ConsumerConfig{
		Config: common.Config{
			Host:        brokerHost,
			Port:        brokerPort,
			Namespace:   os.Getenv("CONSUMER_NAMESPACE"),
			ChannelSize: 128,
		},
		PollIntervalMs:    100,
		PollBackoff:       true,
		MaxPollIntervalMs: 5000,
	}
	producerConfig := common.Config{
		Host:        brokerHost,
		Port:        brokerPort,
		Namespace:   os.Getenv("PRODUCER_NAMESPACE"),
		ChannelSize: 128,
	}

	modelName := os.Getenv("MODEL_NAME")
	if modelName == "" {
		modelName = "gemini-2.5-flash"
	}
	handlerUrl := fmt.Sprintf("http://%s:%s", os.Getenv("HANDLER_URL"), os.Getenv("HANDLER_PORT"))

	ctx := context.Background()
	geminiClient := models.NewGeminiClient(ctx, modelName, handlerUrl)
	if dir, ok := os.LookupEnv("TOOL_CONFIG_DIR"); ok {
		geminiClient.LoadTools(dir)
	}

	consumer := client.NewConsumer(consumerConfig)
	if err := consumer.Connect(); err != nil {
		logger.Fatal("consumer failed to connect to broker", zap.Error(err))
	}
	defer consumer.Disconnect()

	producer := client.NewProducer(producerConfig)
	if err := producer.Connect(); err != nil {
		logger.Fatal("producer failed to connect to broker", zap.Error(err))
	}
	defer producer.Disconnect()

	events := consumer.Subscribe()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				logger.Info("event channel closed, terminating")
				return
			}
			if event.Error != nil {
				logger.Warn("error receiving event", zap.Error(event.Error))
				continue
			}

			var evt supervisor.AgentEvent
			if err := json.Unmarshal(event.Payload, &evt); err != nil {
				logger.Error("unmarshal AgentEvent failed", zap.Error(err))
				continue
			}

			result, err := geminiClient.Query(ctx, evt.Prompt)
			resp := supervisor.AgentResponse{
				RequestID: evt.RequestID,
				AgentName: evt.AgentName,
				Result:    result,
			}
			if err != nil {
				resp.Error = err.Error()
				logger.Error("model query failed", zap.Error(err))
			} else {
				logger.Info("query result",
					zap.String("request_id", evt.RequestID),
					zap.String("agent", evt.AgentName),
					zap.String("result", result))
			}

			data, _ := json.Marshal(resp)
			producer.Produce(data)
		}
	}
}
