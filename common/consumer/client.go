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
	Exchange     string
	Topics       []string
	Queue        string
	WriteTimeout time.Duration
}

type ElasticSearchTarget struct {
	Server string
}

type EcflowClientConsumer struct {
	Source RabbitMQSource
	Target ElasticSearchTarget
	Debug  bool
}

func (s *EcflowClientConsumer) ConsumeMessages() error {
	// create connection to rabbitmq
	log.WithFields(log.Fields{
		"component": "rabbitmq",
		"event":     "connect",
	}).Infof("connecting to rabbitmq...%s", s.Source.Server)
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

	log.WithFields(log.Fields{
		"component": "rabbitmq",
		"event":     "connect",
	}).Info("create exchange... ecflow-client")
	err = channel.ExchangeDeclare(
		s.Source.Exchange,
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
	}).Info("create queue... ecflow-client-queue")
	queue, err := channel.QueueDeclare(
		s.Source.Queue,
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
	for _, topic := range s.Source.Topics {
		err = channel.QueueBind(
			queue.Name,
			topic,
			s.Source.Exchange,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("bind queue has error: %s", err)
		} else {
			log.WithFields(log.Fields{
				"component": "rabbitmq",
				"event":     "connect",
			}).Infof("bind queue...%s", topic)
		}
	}

	// consume messages from rabbitmq
	log.WithFields(log.Fields{
		"component": "rabbitmq",
		"event":     "consume",
	}).Info("start to consume...")
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

	// load message from channel and handle
	go func() {
		// create elasticsearch client.
		ctx := context.Background()
		// can't connect to es in docker without the last two options.
		// see https://github.com/olivere/elastic/issues/824
		client, err := elastic.NewClient(
			elastic.SetURL(s.Target.Server),
			elastic.SetHealthcheck(false),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "elastic",
				"event":     "connect",
			}).Errorf("connect has error: %v", err)
			panic(err)
		}

		for d := range messages {
			// parse message to generate message index
			log.WithFields(log.Fields{
				"component": "consumer",
				"event":     "message",
			}).Debugf("receive message")

			var event common.EventMessage
			err := json.Unmarshal(d.Body, &event)
			if err != nil {
				log.WithFields(log.Fields{
					"component": "consumer",
					"event":     "message",
				}).Errorf("can't create EventMessage: %s", d.Body)
				continue
			}

			messageTime := event.Time
			indexName := messageTime.Format("2006-01-02")

			// send to elasticsearch
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
