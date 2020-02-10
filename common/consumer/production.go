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

type ProductionConsumer struct {
	Source      RabbitMQSource
	Target      ElasticSearchTarget
	WorkerCount int
	BulkSize    int
	Debug       bool
}

func (s *ProductionConsumer) ConsumeMessages() error {
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
		go consumeProductionMessageToElastic(s, messages)
	}

	select {}

	return nil
}

func consumeProductionMessageToElastic(consumer *ProductionConsumer, messages <-chan amqp.Delivery) {
	// create elasticsearch client.
	ctx := context.Background()
	// can't connect to es in docker without the last two options.
	// see https://github.com/olivere/elastic/issues/824
	client, err := elastic.NewClient(
		elastic.SetURL(consumer.Target.Server),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "elastic",
			"event":     "connect",
		}).Errorf("connect has error: %v", err)
		return
	}

	var received []messageWithIndex

	for {
		select {
		case d := <-messages:
			// parse message to generate message index
			log.WithFields(log.Fields{
				"component": "elastic",
				"event":     "message",
			}).Infof("received message...")

			var event common.EventMessage
			err := json.Unmarshal(d.Body, &event)
			if err != nil {
				log.WithFields(log.Fields{
					"component": "consumer",
					"event":     "message",
				}).Errorf("can't create EventMessage: %s", d.Body)
				continue
			}

			indexName := getIndexForProductionMessage(event)

			received = append(received, messageWithIndex{
				indexName, event,
			})
			log.WithFields(log.Fields{
				"component": "elastic",
				"event":     "message",
			}).Infof("receive message...parsed")

			if len(received) > consumer.BulkSize {
				// send to elasticsearch
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "push",
				}).Info("bulk size push...")
				pushMessages(client, received, ctx)
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "push",
				}).Info("bulk size push...done")
				received = nil
			}
		case <-time.After(time.Second * 1):
			if len(received) > 0 {
				// send to elasticsearch
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "push",
				}).Info("time limit push...")
				pushMessages(client, received, ctx)
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "push",
				}).Info("time limit push...done")
				received = nil
			}
		}
	}
}

func getIndexForProductionMessage(event common.EventMessage) string {
	messageTime := event.Time
	indexName := messageTime.Format("2006-01")
	return indexName
}
