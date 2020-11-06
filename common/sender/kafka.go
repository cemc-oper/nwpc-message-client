package sender

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
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
	log.Debug("creating writer...")

	w := kafka.Writer{
		Addr:         kafka.TCP(s.Target.Brokers...),
		Topic:        s.Target.Topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: s.Target.WriteTimeout,
	}

	//w := kafka.NewWriter(kafka.WriterConfig{
	//	Brokers:      s.Target.Brokers,
	//	Topic:        s.Target.Topic,
	//	Balancer:     &kafka.LeastBytes{},
	//	WriteTimeout: s.Target.WriteTimeout,
	//})

	//log.Debug("creating writer...done")
	//
	//log.Debug("sending message...")

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: message,
		},
	)

	if err != nil {
		return fmt.Errorf("send message failed: %s", err)
	}

	//log.Info("sending message...done")
	//log.Debug("closing writer...")

	w.Close()

	//log.Debug("closing writer...done")

	return nil
}
