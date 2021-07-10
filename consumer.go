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

var consumerPool = NewConsumerPool(*consumerPoolLimit)

func PullMessages(ctx context.Context, forUser string) ([]string, error) {

	tryAcquireConsumer := func(ctx context.Context) (*kafka.Consumer, error) {
		for n := 0; n <= 5; n++ {
			c, err := consumerPool.Acquire(ctx)
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

	c, err := tryAcquireConsumer(ctx)
	if err != nil {
		return nil, err
	}
	defer consumerPool.Release(c)

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

	messages := make([]string, 0, *messageLimit)
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
					log.Printf("Waiting (%v) for other messages", *messageReturnLinger)
					ctx, cancel = context.WithTimeout(ctx, *messageReturnLinger)
					defer cancel()
				}
			default:
				log.Printf("Pull no-message: %v", x)
			}
		}
	}

}
