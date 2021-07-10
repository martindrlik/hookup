package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var producerPool = NewProducerPool(*producerPoolLimit)

func PostMessage(ctx context.Context, message, from, to string) error {

	tryAcquireProducer := func(ctx context.Context) (*kafka.Producer, error) {
		for n := 0; n <= 5; n++ {
			p, err := producerPool.Acquire(ctx)
			if err == nil {
				return p, nil
			}
			if err == ctx.Err() {
				return nil, err
			}
			log.Printf("PostMessage failed to acquire producer (attempt: %v): %v", n, err)
			time.Sleep(time.Duration(math.Exp(float64(n))) * time.Second)
		}
		return nil, errors.New("all attempts to acquire producer failed")
	}

	p, err := tryAcquireProducer(ctx)
	if err != nil {
		return err
	}
	defer producerPool.Release(p)

	delivery := make(chan kafka.Event)

	topic := topicFromUsername(to)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value:   []byte(message),
		Headers: []kafka.Header{{Key: "from", Value: []byte(from)}},
	}, delivery)

	e := <-delivery
	m := e.(*kafka.Message)
	defer close(delivery)

	if m.TopicPartition.Error != nil {
		return fmt.Errorf("unable to deliver message: %w", m.TopicPartition.Error)
	}

	return nil

}
