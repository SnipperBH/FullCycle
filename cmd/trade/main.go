package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"v2/internal/infra/kafka"
	"v2/internal/market/dto"
	"v2/internal/market/entity"
	"v2/internal/market/transformer"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ordersIn := make(chan *entity.Order)
	ordersOut := make(chan *entity.Order)
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	kafkaMsgChan := make(chan *ckafka.Message)
	configMap := &ckafka.ConfigMap{
		"bootstrap.servers":        "host.docker.internal:9094",
		"group.id":                 "myGroup",
		"auto.offset.reset":        "latest",
		"session.timeout.ms":       6000,
		"go.events.channel.enable": true,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  1000,
	}

	producer := kafka.NewKafkaProducer(configMap)
	kafka := kafka.NewConsumer(configMap, []string{"input"})

	go kafka.Consume(kafkaMsgChan) //o Go cria uma nova Thread

	//Recebe do canal do kafka, joga no input, processa e joga no output
	//Depois publica no kafka
	book := entity.NewBook(ordersIn, ordersOut, wg)
	go book.Trade() //o Go cria uma nova Thread

	go func() {
		for msg := range kafkaMsgChan {
			wg.Add(1)
			fmt.Println(string(msg.Value))
			tradeInput := dto.TradeInput{}
			err := json.Unmarshal(msg.Value, &tradeInput)
			if err != nil {
				panic(err)
			}
			order := transformer.TransformInput(tradeInput)
			ordersIn <- order
		}
	}()

	for res := range ordersOut {
		output := transformer.TransformOutput(res)
		outputJson, err := json.MarshalIndent(output, "", "  ")
		fmt.Println(string(outputJson))
		if err != nil {
			fmt.Println(err)
		}
		producer.Publish(outputJson, []byte("orders"), "output")
	}
}
