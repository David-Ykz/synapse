package main

import (
	client "synapse/client/internal"
	"synapse/common"
	"time"
)

func main() {
	config1 := client.Config{
		Host:             "localhost",
		Port:             8080,
		PacketType:       common.PRODUCER_MESSAGE,
		DefaultNamespace: "namespace1",
	}
	config2 := client.Config{
		Host:             "localhost",
		Port:             8080,
		PacketType:       common.CONSUMER_MESSAGE,
		DefaultNamespace: "namespace1",
		PollIntervalMs:   1000,
	}
	config3 := client.Config{
		Host:              "localhost",
		Port:              8080,
		PacketType:        common.CONSUMER_MESSAGE,
		DefaultNamespace:  "namespace2",
		PollIntervalMs:    100,
		MaxPollIntervalMs: 2000,
		PollBackoff:       true,
	}

	p1 := client.NewClient(config1)
	p2 := client.NewClient(config2)
	p3 := client.NewClient(config3)
	p1.Connect()
	p2.Connect()
	p3.Connect()

	p1.Push("", []byte("Hello World"))
	time.Sleep(1 * time.Second)

	go p2.Poll("")
	go p3.Poll("")

	time.Sleep(10 * time.Second)

	p1.Disconnect("")
	p2.Disconnect("")
	p3.Disconnect("")

}
