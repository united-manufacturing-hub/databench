package main

import (
	"fmt"
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"go.elastic.co/ecszap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"time"
)

var logger *zap.Logger

// Init initializes the logger
func initZAP() {
	encoderConfig := ecszap.EncoderConfig{
		EnableStackTrace: true,
		EnableCaller:     true,
		EncodeLevel:      zapcore.CapitalColorLevelEncoder,
		EncodeDuration:   zapcore.NanosDurationEncoder,
		EncodeCaller:     ecszap.FullCallerEncoder,
	}
	logger = zap.New(
		ecszap.NewCore(encoderConfig, os.Stdout, zap.DebugLevel),
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel))
	zap.ReplaceGlobals(logger)
}

func main() {
	initZAP()
	defer logger.Sync()

	var brokers []string
	for i := 0; i < 10; i++ {
		url, set := os.LookupEnv(fmt.Sprintf("KAFKA_BROKER_URL_%d", i))
		if !set {
			break
		}
		brokers = append(brokers, url)
	}

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

	for i := 0; i < 16; i++ {
		go enqueueData(generator, client)
	}

	time.Sleep(10 * time.Second)

	zap.S().Infof("Kafka Queue Size: %d", client.GetQueueLength())

	err = client.Close()
	if err != nil {
		zap.S().Fatal(err)
	}
}

func enqueueData(generator *Generator, client *kafka.Client) {
	msg := generator.GetMessage()

	err := client.EnqueueMessage(msg)
	if err != nil {
		zap.S().Fatal(err)
	}
}
