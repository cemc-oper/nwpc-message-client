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

type EcflowClientConsumer struct {
	Source      RabbitMQSource
	Target      ElasticSearchTarget
	WorkerCount int
	BulkSize    int
	Debug       bool
}

func (s *EcflowClientConsumer) ConsumeMessages() error {
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
		go consumeMessageToElastic(s, messages)
	}

	select {}

	return nil
}

func consumeMessageToElastic(consumer *EcflowClientConsumer, messages <-chan amqp.Delivery) {
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
			//log.WithFields(log.Fields{
			//	"component": "elastic",
			//	"event":     "message",
			//}).Infof("received message...")

			var event common.EventMessage
			err := json.Unmarshal(d.Body, &event)
			if err != nil {
				log.WithFields(log.Fields{
					"component": "consumer",
					"event":     "message",
				}).Errorf("can't create EventMessage: %s", d.Body)
				continue
			}

			indexName := getIndexForEcflowClientMessage(event)

			received = append(received, messageWithIndex{
				indexName, event,
			})
			//log.WithFields(log.Fields{
			//	"component": "elastic",
			//	"event":     "message",
			//}).Infof("receive message...parsed")

			if len(received) > consumer.BulkSize {
				// send to elasticsearch
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "push",
				}).Info("bulk size push...")
				err := pushMessages(client, received, ctx)
				if err != nil {
					log.WithFields(log.Fields{
						"component": "elastic",
						"event":     "push",
					}).Warn("bulk size push...failed")
				} else {
					log.WithFields(log.Fields{
						"component": "elastic",
						"event":     "push",
					}).Infof("bulk size push...done, %d", len(received))
					received = nil
				}
			}
		case <-time.After(time.Second * 1):
			if len(received) > 0 {
				// send to elasticsearch
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "push",
				}).Info("time limit push...")
				err := pushMessages(client, received, ctx)
				if err != nil {
					log.WithFields(log.Fields{
						"component": "elastic",
						"event":     "push",
					}).Warn("time limit push...failed")
				} else {
					log.WithFields(log.Fields{
						"component": "elastic",
						"event":     "push",
					}).Infof("time limit push...done, %d", len(received))
					received = nil
				}
			}
		}
		if len(received) >= consumer.BulkSize*10 {
			log.WithFields(log.Fields{
				"component": "elastic",
				"event":     "push",
			}).Fatalf("Count of received messages is larger than %d times of bulk size: %d", 10, len(received))
		}
	}
}

func getIndexForEcflowClientMessage(event common.EventMessage) string {
	messageTime := event.Time
	indexName := fmt.Sprintf("ecflow-client-%s", messageTime.Format("2006-01-02"))
	return indexName
}
