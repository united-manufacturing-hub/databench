package main

import (
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"go.uber.org/zap"
	"math/rand"
	"strconv"
	"time"
)

func CreateKafka(brokers []string, s string) *kafka.Client {
	id := "data-bench-sender" + s + strconv.FormatInt(int64(rand.Int()), 10)
	client, err := kafka.NewKafkaClient(&kafka.NewClientOptions{
		ListenTopicRegex:        nil,
		SenderTag:               kafka.SenderTag{},
		ConsumerGroupId:         "cgi-" + s,
		TransactionalID:         "",
		ClientID:                "cid-" + id,
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

	// Wait for kafka
	for {
		if client.Ready() {
			break
		}
		time.Sleep(1 * time.Second)
		zap.S().Info("Waiting for kafka")
	}
	return client
}
