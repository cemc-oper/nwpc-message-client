package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

type RabbitMQSource struct {
	Server       string
	Topic        string
	WriteTimeout time.Duration
}

type ElasticSearchTarget struct {
	Server string
}

type RabbitMQConsumer struct {
	Source RabbitMQSource
	Debug  bool
	Target ElasticSearchTarget
}

func (s *RabbitMQConsumer) ConsumeMessages() error {
	connection, err := amqp.Dial(s.Source.Server)
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

			messageTime := event.Time
			indexName := messageTime.Format("2006-01-02")

			log.WithFields(log.Fields{
				"component": "elastic",
				"event":     "connect",
			}).Infof("connecting... %s", s.Target.Server)

			ctx := context.Background()
			client, err := elastic.NewClient(
				elastic.SetURL(s.Target.Server))
			if err != nil {
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "connect",
				}).Errorf("connect has error: %v", err)
				panic(err)
			}

			messagePut, err := client.Index().
				Index(indexName).
				BodyJson(event).
				Do(ctx)
			if err != nil {
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "index",
				}).Errorf("index has error: %v", err)
			}

			log.WithFields(log.Fields{
				"component": "elastic",
				"event":     "index",
			}).Infof("index finished: %s %s", messagePut.Id, d.Body)
		}
	}()

	select {}

	return nil
}
