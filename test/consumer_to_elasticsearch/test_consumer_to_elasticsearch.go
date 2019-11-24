package main

import (
	"context"
	"encoding/json"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/streadway/amqp"
	"os"
)

func main() {
	Execute()
}

var (
	rabbitmqServer       = ""
	rabbitmqQueueName    = ""
	elasticServer        = ""
	rabbitmqExchangeName = "nwpc-message"
	rabbitmqRouteKey     = "command.ecflow.*"
)

func init() {
	rootCmd.Flags().StringVar(&rabbitmqServer,
		"rabbitmq-server", "", "rabbitmq server")
	rootCmd.Flags().StringVar(&rabbitmqQueueName,
		"rabbitmq-queue-name", "ecflow-client-queue", "rabbitmq queue name")
	rootCmd.Flags().StringVar(&elasticServer,
		"elasticsearch-server", "", "elasticsearch server")

	rootCmd.MarkFlagRequired("rabbitmq-server")
	rootCmd.MarkFlagRequired("rabbitmq-queue-name")
	rootCmd.MarkFlagRequired("elasticsearch-server")

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.999999",
		FullTimestamp:   true,
	})
}

var rootCmd = &cobra.Command{
	Use:   "test_broker",
	Short: "Test broker.",
	Long:  "Test broker.",
	Run: func(cmd *cobra.Command, args []string) {
		Consume()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func Consume() {
	// create connection to rabbitmq
	connection, err := amqp.Dial(rabbitmqServer)
	if err != nil {
		log.WithFields(log.Fields{
			"type":  "connection",
			"event": "dial",
		}).Errorf("%v", err)
		return
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		log.WithFields(log.Fields{
			"type":  "channel",
			"event": "create",
		}).Errorf("%v", err)
		return
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		rabbitmqExchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"type":  "exchange",
			"event": "create",
		}).Errorf("%v", err)
		return
	}

	queue, err := channel.QueueDeclare(
		rabbitmqQueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"type":  "queue",
			"event": "create",
		}).Errorf("%v", err)
		return
	}

	err = channel.QueueBind(
		queue.Name,
		rabbitmqRouteKey,
		rabbitmqExchangeName,
		false,
		nil,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"type":  "queue",
			"event": "bind",
		}).Errorf("%v", err)
		return
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
		log.WithFields(log.Fields{
			"type":  "consume",
			"event": "consume",
		}).Errorf("%v", err)
		return
	}

	// load message from channel and handle
	for i := 0; i < 8; i++ {
		go func() {
			ctx := context.Background()
			client, err := elastic.NewClient(
				elastic.SetURL(elasticServer),
				elastic.SetHealthcheck(false),
				elastic.SetSniff(false),
			)
			if err != nil {
				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "connect",
				}).Errorf("%v", err)
				return
			}

			for d := range messages {
				// parse message to generate message index
				log.WithFields(log.Fields{
					"component": "consumer",
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

				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "message",
				}).Infof("receive message...parsed")

				messageTime := event.Time
				indexName := messageTime.Format("2006-01-02")

				_, err = client.Index().
					Index(indexName).
					BodyJson(event).
					Do(ctx)
				if err != nil {
					log.WithFields(log.Fields{
						"component": "elastic",
						"event":     "index",
					}).Errorf("%v", err)
					return
				}

				log.WithFields(log.Fields{
					"component": "elastic",
					"event":     "message",
				}).Infof("receive message...done")
			}
		}()
	}

	select {}

	return
}
