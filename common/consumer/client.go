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
	}).Info("create exchange... ecflow-client")
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
	}).Info("create queue... ecflow-client-queue")
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
			}).Infof("bind queue...%s", topic)
		}
	}
	return nil
}

type ElasticSearchTarget struct {
	Server string
}

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

type messageWithIndex struct {
	Index   string
	Message common.EventMessage
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
			log.WithFields(log.Fields{
				"component": "elastic",
				"event":     "message",
			}).Infof("receive message...")
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

			received = append(received, messageWithIndex{
				indexName, event,
			})
			log.WithFields(log.Fields{
				"component": "elastic",
				"event":     "message",
			}).Infof("receive message...parsed")

			if len(received) > consumer.BulkSize {
				// send to elasticsearch
				log.Info("push...")
				pushMessages(client, received, ctx)
				log.Info("push...done")
				received = nil
			}
		case <-time.After(time.Second * 1):
			if len(received) > 0 {
				// send to elasticsearch
				log.Info("time limit push...")
				pushMessages(client, received, ctx)
				received = nil
				log.Info("time limit push...done")
			}
		}
	}
}

func pushMessages(client *elastic.Client, messages []messageWithIndex, ctx context.Context) {
	bulkRequest := client.Bulk()
	for _, indexMessage := range messages {
		request := elastic.NewBulkIndexRequest().
			Index(indexMessage.Index).
			Doc(indexMessage.Message)
		bulkRequest.Add(request)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "elastic",
			"event":     "index",
		}).Errorf("%v", err)
		return
	}
}
