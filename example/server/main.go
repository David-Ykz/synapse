package main

import (
	client "synapse/src/client"
	"time"
)

func main() {
	config1 := client.Config{
		Host:             "localhost",
		Port:             8080,
		DefaultNamespace: "user_request",
	}

	p := client.NewBasicClient(config1)
	p.Connect()

	p.Produce("user_request", []byte("Hello World"))

	time.Sleep(10 * time.Second)

	p.Disconnect("")
}
