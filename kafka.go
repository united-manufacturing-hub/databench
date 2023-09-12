package main

import (
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"go.uber.org/zap"
	"time"
)

func CreateKafkaAndGenerator(brokers []string) (*kafka.Client, *Generator) {
	client, err := kafka.NewKafkaClient(&kafka.NewClientOptions{
		ListenTopicRegex:        nil,
		SenderTag:               kafka.SenderTag{},
		ConsumerGroupId:         "",
		TransactionalID:         "",
		ClientID:                "data-bench-sender",
		Brokers:                 brokers,
		StartOffset:             0,
		OpenDeadLine:            0,
		Partitions:              6,
		ReplicationFactor:       3,
		EnableTLS:               false,
		AutoCommit:              true,
		ProducerReturnSuccesses: false,
	})
	if err != nil {
		zap.S().Fatal(err)
	}

	// Create a new generator
	generator, err := NewGenerator()
	if err != nil {
		zap.S().Fatal(err)
	}

	// Wait for kafka
	for {
		if client.Ready() {
			break
		}
		time.Sleep(1 * time.Second)
		zap.S().Info("Waiting for kafka")
	}
	return client, generator
}
