package consumer

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type RabbitMQTarget struct {
	Server       string
	Topic        string
	WriteTimeout time.Duration
}

type RabbitMQConsumer struct {
	Target RabbitMQTarget
	Debug  bool
}

func (s *RabbitMQConsumer) ConsumeMessages() error {
	connection, err := amqp.Dial(s.Target.Server)
	if err != nil {
		return fmt.Errorf("dial to rabbitmq has error: %s", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("create channel has error: %s", err)
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		"ecflow-client",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("create exchange has error: %s", err)
	}

	queue, err := channel.QueueDeclare(
		"ecflow-client-queue",
		false,
		false,
		true,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("create queue has error: %s", err)
	}

	err = channel.QueueBind(
		queue.Name,
		"",
		"ecflow-client",
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("bind queue has error: %s", err)
	}

	messages, err := channel.Consume(
		queue.Name,
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

	go func() {
		for d := range messages {
			var event common.EventMessage
			err := json.Unmarshal(d.Body, &event)
			if err != nil {
				log.WithFields(log.Fields{
					"component": "rabbitmq",
					"event":     "message",
				}).Errorf("can't create EventMessage: %s", d.Body)
				continue
			}
			log.WithFields(log.Fields{
				"component": "rabbitmq",
				"event":     "message",
			}).Infof("EventMessage: %s", event)
		}
	}()

	select {}

	return nil
}
