package app

import (
	"github.com/nwpc-oper/nwpc-message-client/common/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	//rootCmd.AddCommand(productionCmd)

	productionCmd.Flags().StringVar(&rabbitmqServer,
		"rabbitmq-server", "", "rabbitmq server")
	productionCmd.Flags().StringVar(&rabbitmqQueueName,
		"rabbitmq-queue-name", "", "rabbitmq queue name")

	productionCmd.Flags().StringVar(&consumerType,
		"consumer-type", "print", "consumer type")

	productionCmd.Flags().IntVar(&workerCount, "worker-count", 2, "worker count")

	productionCmd.Flags().StringVar(&elasticServer,
		"elasticsearch-server", "", "elasticsearch server")
	productionCmd.Flags().IntVar(&bulkSize, "bulk-size", 20, "bulk size")

	productionCmd.Flags().BoolVar(&isDebug, "debug", true, "debug mode")

	productionCmd.MarkFlagRequired("rabbitmq-server")
	productionCmd.MarkFlagRequired("rabbitmq-queue-name")
}

const productionLongDescription = `
Consume production message from rabbitmq and store them into elasticsearch.
`

var productionCmd = &cobra.Command{
	Use:   "production",
	Short: "consume production message",
	Long:  productionLongDescription,
	RunE: func(cmd *cobra.Command, args []string) error {
		var currentConsumer consumer.Consumer = nil
		currentSource := consumer.RabbitMQSource{
			Server:   rabbitmqServer,
			Exchange: "nwpc.operation.production",
			Topics:   []string{"*.production.*"},
			Queue:    rabbitmqQueueName,
		}

		if consumerType == "print" {
			currentConsumer = createPrinterConsumer(currentSource, workerCount, isDebug)
		} else if consumerType == "elasticsearch" {
			target := consumer.ElasticSearchTarget{
				Server: elasticServer,
			}
			currentConsumer = createElasticSearchConsumer(currentSource, target, workerCount, isDebug)
		}

		if currentConsumer == nil {
			log.Fatal("consumer type is not supported: %s", consumerType)
			return nil
		}

		log.WithFields(log.Fields{
			"component": "production",
			"event":     "consumer",
		}).Info("start to consume...")

		err := currentConsumer.ConsumeMessages()
		if err != nil {
			log.WithFields(log.Fields{
				"component": "production",
				"event":     "consumer",
			}).Errorf("%v", err)
		}
		return err
	},
}

func createPrinterConsumer(source consumer.RabbitMQSource, workerCount int, debug bool) *consumer.PrinterConsumer {
	printerConsumer := &consumer.PrinterConsumer{
		Source:      source,
		WorkerCount: workerCount,
		Debug:       debug,
	}
	return printerConsumer
}

func createElasticSearchConsumer(
	source consumer.RabbitMQSource,
	target consumer.ElasticSearchTarget,
	workerCount int, debug bool) *consumer.ProductionConsumer {
	elasticSearchConsumer := &consumer.ProductionConsumer{
		Source:      source,
		Target:      target,
		WorkerCount: workerCount,
		BulkSize:    bulkSize,
		Debug:       debug,
	}
	return elasticSearchConsumer
}
