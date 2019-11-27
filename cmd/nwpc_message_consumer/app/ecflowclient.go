package app

import (
	"github.com/nwpc-oper/nwpc-message-client/common/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(ecflowClientCmd)

	ecflowClientCmd.Flags().StringVar(&rabbitmqServer,
		"rabbitmq-server", "", "rabbitmq server")
	ecflowClientCmd.Flags().StringVar(&rabbitmqQueueName,
		"rabbitmq-queue-name", "ecflow-client-queue", "rabbitmq queue name")
	ecflowClientCmd.Flags().StringVar(&elasticServer,
		"elasticsearch-server", "", "elasticsearch server")
	ecflowClientCmd.Flags().IntVar(&workerCount, "worker-count", 2, "worker count")
	ecflowClientCmd.Flags().IntVar(&bulkSize, "bulk-size", 20, "bulk size")
	ecflowClientCmd.Flags().BoolVar(&isDebug, "debug", true, "debug mode")

	rootCmd.MarkFlagRequired("rabbitmq-server")
	rootCmd.MarkFlagRequired("rabbitmq-queue-name")
	rootCmd.MarkFlagRequired("elastic-server")
}

const longDescription = `
Consume ecflow_client message from rabbitmq and store them into elasticsearch.

The command will use elasticsearch's Bulk API to send multiply messages 
when some count of messages (default 20) are received or time limit (1 second) is reached.

The command run multiply goroutines (like threads, default is 2) to receive messages.
`

var ecflowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "consume message from ecflow client command",
	Long:  longDescription,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "consumer",
		}).Info("start to consume...")

		consumer := &consumer.EcflowClientConsumer{
			Source: consumer.RabbitMQSource{
				Server:   rabbitmqServer,
				Exchange: "nwpc-message",
				Topics:   []string{"command.ecflow.ecflow_client"},
				Queue:    rabbitmqQueueName,
			},
			Target: consumer.ElasticSearchTarget{
				Server: elasticServer,
			},
			WorkerCount: workerCount,
			BulkSize:    bulkSize,
			Debug:       isDebug,
		}

		err := consumer.ConsumeMessages()
		if err != nil {
			log.WithFields(log.Fields{
				"component": "ecflow-client",
				"event":     "consumer",
			}).Errorf("%v", err)
		}
		return err
	},
}
