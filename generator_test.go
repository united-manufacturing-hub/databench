package main

import (
	"testing"
	"time"
)

func TestNewGenerator(t *testing.T) {
	generator, err := NewGenerator()
	if err != nil {
		t.Fatal(err)
	}
	if generator == nil {
		t.Fatal("generator is nil")
	}

	if len(generator.topics) != 100_000 {
		t.Fatalf("expected 100_000 topics, got %d", len(generator.topics))
	}

	// Show the first 10 topics
	for i := 0; i < 10; i++ {
		t.Log(generator.topics[i])
	}
}

const nMessages = 1_000_000

func TestGeneratorSpeed(t *testing.T) {
	timeUsed := time.Duration(0)
	nTests := 10
	topTest := time.Duration(99999999999999)
	botTest := time.Duration(0)
	for i := 0; i < nTests; i++ {
		testTime := test(t)
		timeUsed += testTime
		t.Logf("Test %d took %s", i, testTime)
		if testTime > botTest {
			botTest = testTime
		}
		if testTime < topTest {
			topTest = testTime
		}
	}
	t.Logf("Messages per second: %f", float64(nMessages*nTests)/float64(timeUsed.Milliseconds())*1000)
	t.Logf("Top test: %s", topTest)
	t.Logf("Bot test: %s", botTest)
}

func test(t *testing.T) time.Duration {
	generator, err := NewGenerator()
	if err != nil {
		t.Fatal(err)
	}
	if generator == nil {
		t.Fatal("generator is nil")
	}
	time.Sleep(1 * time.Second)

	messages := make([]Message, nMessages)

	start := time.Now()
	for i := 0; i < nMessages; i++ {
		messages[i] = generator.GetMessage()
	}
	end := time.Now()

	return end.Sub(start)
}
