package consumer

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type RabbitMQSource struct {
	Server       string
	Exchange     string
	Topics       []string
	Queue        string
	WriteTimeout time.Duration

	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

func (source *RabbitMQSource) CreateConnection() error {
	log.WithFields(log.Fields{
		"component": "rabbitmq",
		"event":     "connect",
	}).Infof("connecting to rabbitmq...%s", source.Server)
	var err error
	source.connection, err = amqp.Dial(source.Server)
	if err != nil {
		return fmt.Errorf("dial to rabbitmq has error: %s", err)
	}

	source.channel, err = source.connection.Channel()
	if err != nil {
		return fmt.Errorf("create channel has error: %s", err)
	}

	log.WithFields(log.Fields{
		"component": "rabbitmq",
		"event":     "connect",
	}).Infof("create exchange...%s", source.Exchange)
	err = source.channel.ExchangeDeclare(
		source.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("create exchange has error: %s", err)
	}

	log.WithFields(log.Fields{
		"component": "rabbitmq",
		"event":     "connect",
	}).Infof("create queue...%s", source.Queue)
	source.queue, err = source.channel.QueueDeclare(
		source.Queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("create queue has error: %s", err)
	}

	log.WithFields(log.Fields{
		"component": "rabbitmq",
		"event":     "connect",
	}).Info("bind queues...")
	for _, topic := range source.Topics {
		err = source.channel.QueueBind(
			source.queue.Name,
			topic,
			source.Exchange,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("bind queue has error: %s", err)
		} else {
			log.WithFields(log.Fields{
				"component": "rabbitmq",
				"event":     "connect",
			}).Infof("bind queue...done, %s", topic)
		}
	}
	return nil
}
