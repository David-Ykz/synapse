package main

import (
	"log"
	"os"
	"strconv"

	"github.com/David-Ykz/synapse/client"
	"github.com/David-Ykz/synapse/common"
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

func callLLM(input []byte) ([]byte, error) {
	return input, nil
}

func main() {
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

			result, err := callLLM(event.Payload)
			log.Printf("Result: %s\n", result)
			if err != nil {
				log.Printf("Error calling model: %v", err)
				continue
			}

			producer.Produce(result)
			log.Println("Result written back to producer")
		}
	}
}
