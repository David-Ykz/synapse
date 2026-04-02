package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	client "synapse/client"
	common "synapse/common"
	models "synapse/workers/models"
)

var (
	producerConfig = common.Config{
		Host:        "localhost",
		Port:        8080,
		Namespace:   "template-output",
		ChannelSize: 128,
	}
	consumerConfig = client.ConsumerConfig{
		Config: common.Config{
			Host:        "localhost",
			Port:        8080,
			Namespace:   "template-input",
			ChannelSize: 128,
		},
		PollIntervalMs:    100,
		PollBackoff:       true,
		MaxPollIntervalMs: 5000,
	}
)

func main() {
	// temporary hard limit
	modelCap := 5

	brokerHost := os.Getenv("BROKER_HOST")
	if brokerHost != "" {
		consumerConfig.Host = brokerHost
		producerConfig.Host = brokerHost
	}
	brokerPort := os.Getenv("BROKER_PORT")
	if brokerPort != "" {
		consumerConfig.Port, _ = strconv.Atoi(brokerPort)
		producerConfig.Port, _ = strconv.Atoi(brokerPort)
	}
	consumerConfig.Namespace = os.Getenv("CONSUMER_NAMESPACE")
	producerConfig.Namespace = os.Getenv("PRODUCER_NAMESPACE")

	modelName := os.Getenv("MODEL_NAME")
	if modelName == "" {
		modelName = "gemini-2.5-flash"
	}
	handlerEndpoint := os.Getenv("HANDLER_URL")
	handlerPort := os.Getenv("HANDLER_PORT")
	handlerUrl := fmt.Sprintf("http://%s:%s", handlerEndpoint, handlerPort)

	// initialize llm
	ctx := context.Background()
	geminiClient := models.NewGeminiClient(ctx, modelName, handlerUrl)
	toolConfigDir, ok := os.LookupEnv("TOOL_CONFIG_DIR")
	if ok {
		geminiClient.LoadTools(toolConfigDir)
	}

	// initialize consumer
	consumer := client.NewConsumer(consumerConfig)
	if err := consumer.Connect(); err != nil {
		log.Fatalf("Failed to connect consumer: %v", err)
	}
	defer consumer.Disconnect()

	// initialize producer
	producer := client.NewProducer(producerConfig)
	if err := producer.Connect(); err != nil {
		log.Fatalf("Failed to connect producer: %v", err)
	}
	defer producer.Disconnect()

	// listen for events
	events := consumer.Subscribe()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				log.Println("Event channel closed")
				return
			}
			if event.Error != nil {
				log.Printf("Error receiving event: %v", event.Error)
				continue
			}
			if modelCap > 0 {
				result, err := geminiClient.Query(ctx, string(event.Payload))
				modelCap -= 1
				log.Printf("Result: %s\n", result)
				if err != nil {
					log.Printf("Error calling model: %w", err)
					continue
				}
				producer.Produce([]byte(result))
				log.Println("Result written back to producer")
			}
		}
	}
}
