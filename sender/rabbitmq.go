package sender

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type RabbitMQTarget struct {
	Server       string
	Topic        string
	WriteTimeout time.Duration
}

type RabbitMQSender struct {
	Target RabbitMQTarget
	Debug  bool
}

func (s *RabbitMQSender) SendMessage(message []byte) error {
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

	err = channel.Publish(
		"ecflow-client",
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})
	if err != nil {
		return fmt.Errorf("publish message has error: %s", err)
	}

	return nil
}
