package hookup

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type registerUsersError []error

var (
	ErrUserAlreadyExists = errors.New("user already exists")
)

func (err registerUsersError) Error() string {
	return "unable to register some or all users"
}

func (h *Hookup) RegisterUsers(names []string) error {

	topics := make([]kafka.TopicSpecification, 0, len(names))
	for _, name := range names {
		topics = append(topics, kafka.TopicSpecification{
			Topic:             topicFromUsername(name),
			NumPartitions:     2,
			ReplicationFactor: 1})
	}

	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": h.options.broker})
	if err != nil {
		return err
	}
	defer a.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic(err)
	}
	results, err := a.CreateTopics(
		ctx,
		topics,
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		return err
	}

	for _, result := range results {

		log.Printf("RegisterUsers result: %v", result)

		switch result.Error.Code() {
		case kafka.ErrTopicAlreadyExists:
			return ErrUserAlreadyExists
		case kafka.ErrNoError:
			return nil
		}
	}

	return nil

}

const (
	userTopicPrefix = "user-"
	userTopicSuffix = "-messages"
)

func topicFromUsername(name string) string { return userTopicPrefix + name + userTopicSuffix }
func usernameFromTopic(topic string) string {
	s := strings.TrimPrefix(topic, userTopicPrefix)
	s = strings.TrimSuffix(s, userTopicSuffix)
	return s
}
