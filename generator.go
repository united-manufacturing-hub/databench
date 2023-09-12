package main

import (
	"DataBencher/powerplant"
	"encoding/hex"
	"github.com/goccy/go-json"
	"github.com/united-manufacturing-hub/Sarama-Kafka-Wrapper/pkg/kafka"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type TopicInternal struct {
	Name string
	Unit powerplant.Unit
	Type powerplant.Type
}

const TopicAmount = 100_000
const SplitPoint = 4
const CacheSize = 100_000

type Generator struct {
	topics    []TopicInternal
	dataChan  chan kafka.Message
	running   bool
	requested atomic.Uint64
}

func NewGenerator() (*Generator, error) {
	chernobylnuclearpowerplant, err := powerplant.Load()
	if err != nil {
		return nil, err
	}
	// umh.v1.<enterprise>.<site>.<area>.<productionLine>.<workCell>.<tagGroup>.<tag>
	var generator Generator
	generator.topics = make([]TopicInternal, 0, TopicAmount)
	generator.running = true
	generator.requested = atomic.Uint64{}

	var sb strings.Builder
	for i := 0; i < TopicAmount; i++ {
		sb.WriteString("umh.v1.")
		sb.WriteString(chernobylnuclearpowerplant.Enterprise)
		sb.WriteRune('.')

		// site based on random
		site := chernobylnuclearpowerplant.Sites[0]
		sb.WriteString(site.Site)
		sb.WriteByte(byte(rand.Intn(4) + 49))
		sb.WriteRune('.')

		// area based on random
		area := randEntry(&site.Areas)
		sb.WriteString(area.Area)
		sb.WriteRune('.')

		// productionLine based on random
		productionLine := randEntry(&area.ProductionLines)
		sb.WriteString(productionLine.ProductionLine)
		sb.WriteRune('.')

		// workCell based on random
		workCell := randEntry(&productionLine.WorkCells)
		sb.WriteString(workCell.WorkCell)
		sb.WriteRune('.')

		// read tagGroup from workCell
		tagGroup := workCell.TagGroup
		sb.WriteString(tagGroup)
		sb.WriteRune('.')

		// tag based on random
		tag := randEntry(&workCell.Tags)
		sb.WriteString(tag.Name)

		var t TopicInternal

		t.Unit = tag.Unit
		t.Type = tag.Type

		// Add random 6 digit hex number to make topic unique
		sb.WriteRune('_')
		hexBytes := make([]byte, 3)
		rand.Read(hexBytes)
		sb.WriteString(hex.EncodeToString(hexBytes))

		t.Name = sb.String()

		generator.topics = append(generator.topics, t)
		sb.Reset()
	}

	generator.dataChan = make(chan kafka.Message, CacheSize)
	for i := 0; i < 64; i++ {
		go generator.generate()
	}

	return &generator, nil
}

func (g *Generator) generate() {
	topic := randEntry(&g.topics)
	var m kafka.Message
	var keyStringBuilder strings.Builder
	var randomGen = rand.New(rand.NewSource(time.Now().UnixNano()))

	data := make(map[string]interface{})

	header := make(map[string][]byte)
	header["X-Origin"] = []byte("generator")

	for g.running {
		keyStringBuilder.Reset()
		clear(data)
		// split topic at SPLIT_POINT into Topic and Key
		splits := strings.Split(topic.Name, ".")

		//m.Topic = topic.Name[:SplitPoint]
		m.Topic = strings.Join(splits[:SplitPoint], ".")
		keyStringBuilder.WriteString(strings.Join(splits[:SplitPoint], "."))

		// Append unix nano time to key, preventing imbalance in kafka partitions
		keyStringBuilder.WriteRune('.')
		nanoTime := time.Now().UnixNano()
		keyStringBuilder.WriteString(strconv.FormatInt(nanoTime, 10))
		m.Key = []byte(keyStringBuilder.String())

		data["timestamp_ms"] = nanoTime / 1_000_000
		switch topic.Unit {
		case powerplant.UnitNone:
			switch topic.Type {
			case powerplant.Boolean:
				data["value"] = randomGen.Intn(2) == 1
			case powerplant.Float:
				data["value"] = randomGen.Float64()
			case powerplant.Int:
				data["value"] = randomGen.Int()
			default:
				panic("unknown type")
			}
		case powerplant.UnitDegreeC:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit degreeC")
			case powerplant.Float:
				data["degreeC"] = randomGen.Float64() * 1000
			case powerplant.Int:
				data["degreeC"] = randomGen.Intn(1000)
			default:
				panic("unknown type")
			}
		case powerplant.UnitPercent:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit percent")
			case powerplant.Float:
				data["percent"] = randomGen.Float64() * 100
			case powerplant.Int:
				data["percent"] = randomGen.Intn(100)
			default:
				panic("unknown type")
			}
		case powerplant.UnitPascal:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit pascal")
			case powerplant.Float:
				data["pascal"] = randomGen.Float64() * 1000
			case powerplant.Int:
				data["pascal"] = randomGen.Intn(1000)
			default:
				panic("unknown type")
			}
		case powerplant.UnitCubicMetersPerHour:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit cubicMetersPerHour")
			case powerplant.Float:
				data["cubicMetersPerHour"] = randomGen.Float64() * 1000
			case powerplant.Int:
				data["cubicMetersPerHour"] = randomGen.Intn(1000)
			default:
				panic("unknown type")
			}
		case powerplant.UnitVolt:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit volt")
			case powerplant.Float:
				data["volt"] = randomGen.Float64() * 1000
			case powerplant.Int:
				data["volt"] = randomGen.Intn(1000)
			default:
				panic("unknown type")
			}
		case powerplant.UnitAmpere:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit ampere")
			case powerplant.Float:
				data["ampere"] = randomGen.Float64() * 1000
			case powerplant.Int:
				data["ampere"] = randomGen.Intn(1000)
			default:
				panic("unknown type")
			}
		case powerplant.UnitSivertPerHour:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit sivertPerHour")
			case powerplant.Float:
				data["sivertPerHour"] = randomGen.Float64()
			case powerplant.Int:
				data["sivertPerHour"] = randomGen.Intn(1)
			default:
				panic("unknown type")
			}
		case powerplant.UnitRotationsPerMinute:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit rotationsPerMinute")
			case powerplant.Float:
				data["rotationsPerMinute"] = randomGen.Float64() * 1000
			case powerplant.Int:
				data["rotationsPerMinute"] = randomGen.Intn(1000)
			default:
				panic("unknown type")
			}
		case powerplant.UnitWatt:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit watt")
			case powerplant.Float:
				data["watt"] = randomGen.Float64() * 1000 * 1000
			case powerplant.Int:
				data["watt"] = randomGen.Intn(1000_0000)
			default:
				panic("unknown type")
			}
		case powerplant.UnitSpeed:
			switch topic.Type {
			case powerplant.Boolean:
				panic("boolean not allowed with unit speed")
			case powerplant.Float:
				data["speed"] = randomGen.Float64()
			case powerplant.Int:
				data["speed"] = randomGen.Intn(1)
			}
		}

		m.Value, _ = json.Marshal(data)
		// This is not fully correct, but it is good enough
		header["X-Trace"] = []byte(strconv.FormatInt(nanoTime/1_000_000, 10))

		m.Header = header

		g.dataChan <- m
	}
}

func (g *Generator) GetMessage() kafka.Message {
	g.requested.Add(1)
	return <-g.dataChan
}

func (g *Generator) Stop() {
	g.running = false
	// Cleanup dataChan
	for len(g.dataChan) > 0 {
		<-g.dataChan
	}
}

func (g *Generator) GetRequested() uint64 {
	return g.requested.Load()
}

func randEntry[T any](entries *[]T) T {
	lenEntries := len(*entries)
	if lenEntries == 0 {
		panic("entries is empty")
	}
	return (*entries)[rand.Intn(lenEntries)]
}
