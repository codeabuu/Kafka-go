package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"
	consumer, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}
	consumer.ConsumerPartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Println("consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneCh := make(chan struct{})

	go func ()  {
		for {
			select {
			case err := <-consumer.Error():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message count: %d: | Topic (%s) | Message(%s)n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.println("Interuption detected")
				doneCh <- struct{{}}
			
			}
		}
	}()
}
