package sender

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaTarget struct {
	Brokers      []string
	Topic        string
	WriteTimeout time.Duration
}

type KafkaSender struct {
	Target KafkaTarget
	Debug  bool
}

func (s *KafkaSender) SendMessage(message []byte) error {
	w := kafka.Writer{
		Addr:         kafka.TCP(s.Target.Brokers...),
		Topic:        s.Target.Topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: s.Target.WriteTimeout,
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: message,
		},
	)

	if err != nil {
		return fmt.Errorf("send message failed: %s", err)
	}

	w.Close()

	return nil
}
