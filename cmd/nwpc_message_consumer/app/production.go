package app

import (
	"github.com/nwpc-oper/nwpc-message-client/common/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(productionCmd)

	productionCmd.Flags().StringVar(&rabbitmqServer,
		"rabbitmq-server", "", "rabbitmq server")
	productionCmd.Flags().StringVar(&rabbitmqQueueName,
		"rabbitmq-queue-name", "", "rabbitmq queue name")
	productionCmd.Flags().IntVar(&workerCount, "worker-count", 2, "worker count")
	productionCmd.Flags().BoolVar(&isDebug, "debug", true, "debug mode")

	productionCmd.MarkFlagRequired("rabbitmq-server")
	productionCmd.MarkFlagRequired("rabbitmq-queue-name")
}

const productionLongDescription = `
Consume production message from rabbitmq and store them into elasticsearch.
`

var productionCmd = &cobra.Command{
	Use:   "production",
	Short: "consume message of production",
	Long:  productionLongDescription,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.WithFields(log.Fields{
			"component": "production",
			"event":     "consumer",
		}).Info("start to consume...")

		consumer := &consumer.PrinterConsumer{
			Source: consumer.RabbitMQSource{
				Server:   rabbitmqServer,
				Exchange: "nwpc.operation.production",
				Topics:   []string{"*.production.*"},
				Queue:    rabbitmqQueueName,
			},
			WorkerCount: workerCount,
			Debug:       isDebug,
		}

		err := consumer.ConsumeMessages()
		if err != nil {
			log.WithFields(log.Fields{
				"component": "production",
				"event":     "consumer",
			}).Errorf("%v", err)
		}
		return err
	},
}
