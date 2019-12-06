package consumer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
)

type PrinterConsumer struct {
	Source      RabbitMQSource
	WorkerCount int
	Debug       bool
}

func (s *PrinterConsumer) ConsumeMessages() error {
	// create connection to rabbitmq
	err := s.Source.CreateConnection()
	if err != nil {
		if s.Source.connection != nil {
			s.Source.connection.Close()
		}
		if s.Source.channel != nil {
			s.Source.channel.Close()
		}
		return err
	}

	defer s.Source.connection.Close()
	defer s.Source.channel.Close()

	// consume messages from rabbitmq
	log.WithFields(log.Fields{
		"component": "rabbitmq",
		"event":     "consume",
	}).Info("start to consume...")
	messages, err := s.Source.channel.Consume(
		s.Source.queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to register a consumer: %s", err)
	}

	// load message from channel and handle
	for i := 0; i < s.WorkerCount; i++ {
		go func() {
			for message := range messages {
				log.Infof("%s", message.Body)
			}
		}()
	}

	select {}
}
