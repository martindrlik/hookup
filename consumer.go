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

func (h *Hookup) PullMessages(ctx context.Context, forUser string) ([]string, error) {

	c, err := h.tryAcquireConsumer(ctx)
	if err != nil {
		return nil, err
	}
	defer h.consumerPool.Release(c)

	topic := topicFromUsername(forUser)

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to subscribe topic: %w", err)
	}

	defer c.Unsubscribe()

	commit := func() {
		_, err = c.Commit()
		if err != nil {
			log.Println(err)
		}
	}

	messages := make([]string, 0, h.options.pullLimit)
	var cancel context.CancelFunc

	for {
		select {
		case <-ctx.Done():
			commit()
			return messages, nil
		default:
			switch x := c.Poll(100).(type) {
			case nil:
			case *kafka.Message:
				from := string(x.Headers[0].Value)
				text := string(x.Value)
				messages = append(messages, fmt.Sprintf("message: %s, from: %s", text, from))

				if len(messages) == cap(messages) {
					commit()
					return messages, nil
				}

				if len(messages) == 1 {
					log.Printf("Waiting (%v) for other messages", h.options.pullLinger)
					ctx, cancel = context.WithTimeout(ctx, h.options.pullLinger)
					defer cancel()
				}
			default:
				log.Printf("Pull no-message: %v", x)
			}
		}
	}

}

func (h *Hookup) tryAcquireConsumer(ctx context.Context) (*kafka.Consumer, error) {

	for n := 0; n <= 5; n++ {
		c, err := h.consumerPool.Acquire(ctx)
		if err == nil {
			return c, nil
		}
		if err == ctx.Err() {
			return nil, ctx.Err()
		}
		log.Printf("Failed to acquire consumer (attempt: %v): %v", n, err)
		time.Sleep(time.Duration(math.Exp(float64(n))) * time.Second)
	}
	return nil, errors.New("all attempts to acquire consumer failed")

}
