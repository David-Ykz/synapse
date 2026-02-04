package main

import (
	client "synapse/client/internal"
	"time"
)

func main() {
	config := client.Config{
		Host:       "localhost",
		Port:       8080,
		ClientType: 1,
		Namespace:  "main",
	}

	p := client.NewClient(config)
	p.Connect()

	for {
		p.Send("Hello World")
		time.Sleep(1 * time.Second)
	}

}
