package main

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProducerPool struct {
	sem  chan struct{}
	idle chan *kafka.Producer
}

func NewProducerPool(limit int) *ProducerPool {

	sem := make(chan struct{}, limit)
	idle := make(chan *kafka.Producer, limit)
	return &ProducerPool{sem, idle}

}

func (pp *ProducerPool) Release(p *kafka.Producer) {
	pp.idle <- p
}

func (pp *ProducerPool) Acquire(ctx context.Context) (*kafka.Producer, error) {

	select {
	case p := <-pp.idle:
		return p, nil
	case pp.sem <- struct{}{}:
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": *broker})
		if err != nil {
			<-pp.sem
		}
		return p, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}
