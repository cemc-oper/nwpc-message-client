package consumer

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
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
			log.Printf("[x] %s\n", d.Body)
		}
	}()

	select {}

	return nil
}
