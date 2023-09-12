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

	testInsertOnly(brokers)
}

func testInsertOnly(brokers []string) {
	zap.S().Info("Testing insert only performance")

	client := CreateKafka(brokers, "tIO")

	// Create a new generator
	generator, err := NewGenerator()
	if err != nil {
		zap.S().Fatal(err)
	}

	zap.S().Info("Beginning load test")

	deadLine := time.Now().Add(60 * time.Second)

	for time.Now().Before(deadLine) {
		enqueueData(generator, client)
	}

	zap.S().Info("Send complete")

	requested := generator.GetRequested()
	qLen := client.GetQueueLength()
	zap.S().Infof("Requested %d messages", requested)
	zap.S().Infof("Kafka Queue Size: %d", qLen)
	zap.S().Infof("Sent %d messages", requested-uint64(qLen))

	zap.S().Info("Waiting for queue to empty")
	start := time.Now()
	for client.GetQueueLength() > 0 {
		time.Sleep(1 * time.Second)
	}
	end := time.Now()
	zap.S().Infof("Queue emptied in %s", end.Sub(start).String())

	err = client.Close()
	if err != nil {
		zap.S().Fatal(err)
	}
	zap.S().Info("Finished insert only performance test")
}

func enqueueData(generator *Generator, client *kafka.Client) {
	msg := generator.GetMessage()

	err := client.EnqueueMessage(msg)
	if err != nil {
		zap.S().Fatal(err)
	}
}
