package main

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumerPool struct {
	sem  chan struct{}
	idle chan *kafka.Consumer
}

func NewConsumerPool(limit int) *ConsumerPool {
	sem := make(chan struct{}, limit)
	idle := make(chan *kafka.Consumer, limit)
	return &ConsumerPool{sem, idle}
}

func (cp *ConsumerPool) Release(c *kafka.Consumer) {
	cp.idle <- c
}

func (cp *ConsumerPool) Acquire(ctx context.Context) (*kafka.Consumer, error) {

	select {
	case c := <-cp.idle:
		return c, nil
	case cp.sem <- struct{}{}:
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":     *broker,
			"broker.address.family": "v4",
			"session.timeout.ms":    6000,
			"group.id":              "default",
			"auto.offset.reset":     "earliest"})
		if err != nil {
			<-cp.sem
		}
		return c, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}
