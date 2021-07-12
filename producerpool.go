package hookup

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type producerPool struct {
	broker string
	sem    chan struct{}
	idle   chan *kafka.Producer
}

func newProducerPool(broker string, limit int) *producerPool {

	sem := make(chan struct{}, limit)
	idle := make(chan *kafka.Producer, limit)
	return &producerPool{broker, sem, idle}

}

func (pp *producerPool) Release(p *kafka.Producer) {
	pp.idle <- p
}

func (pp *producerPool) Acquire(ctx context.Context) (*kafka.Producer, error) {

	select {
	case p := <-pp.idle:
		return p, nil
	case pp.sem <- struct{}{}:
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": pp.broker})
		if err != nil {
			<-pp.sem
		}
		return p, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}
