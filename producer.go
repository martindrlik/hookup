package hookup

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func (h *Hookup) PostMessage(ctx context.Context, message, from, to string) error {

	p, err := h.tryAcquireProducer(ctx)
	if err != nil {
		return err
	}
	defer h.producerPool.Release(p)

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

func (h *Hookup) tryAcquireProducer(ctx context.Context) (*kafka.Producer, error) {

	for n := 0; n <= 5; n++ {
		p, err := h.producerPool.Acquire(ctx)
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
