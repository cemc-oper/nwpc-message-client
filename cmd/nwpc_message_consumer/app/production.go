package app

import (
	"github.com/nwpc-oper/nwpc-message-client/common/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const productionLongDescription = `
Consume production message from rabbitmq and store them into elasticsearch.
`

type productionCommand struct {
	cmd               *cobra.Command
	rabbitmqServer    string
	rabbitmqQueueName string
	elasticServer     string
	workerCount       int
	bulkSize          int
	isDebug           bool
	consumerType      string
}

func (c *productionCommand) getCommand() *cobra.Command {
	return c.cmd
}

func (c *productionCommand) consumeProduction(cmd *cobra.Command, args []string) error {
	var currentConsumer consumer.Consumer = nil
	currentSource := consumer.RabbitMQSource{
		Server:   c.rabbitmqServer,
		Exchange: "nwpc.operation.production",
		Topics:   []string{"*.production.*"},
		Queue:    c.rabbitmqQueueName,
	}

	if c.consumerType == "print" {
		currentConsumer = createPrinterConsumer(currentSource, c.workerCount, c.isDebug)
	} else if c.consumerType == "elasticsearch" {
		target := consumer.ElasticSearchTarget{
			Server: elasticServer,
		}
		currentConsumer = createElasticSearchConsumer(currentSource, target, c.workerCount, c.isDebug)
	}

	if currentConsumer == nil {
		log.Fatal("consumer type is not supported: %s", c.consumerType)
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
}

func newProductionCommand() *productionCommand {
	pc := &productionCommand{}

	productionCmd := &cobra.Command{
		Use:   "production",
		Short: "consume production message",
		Long:  productionLongDescription,
		RunE:  pc.consumeProduction,
	}

	productionCmd.Flags().StringVar(&pc.rabbitmqServer,
		"rabbitmq-server", "", "rabbitmq server")
	productionCmd.Flags().StringVar(&pc.rabbitmqQueueName,
		"rabbitmq-queue-name", "", "rabbitmq queue name")

	productionCmd.Flags().StringVar(&pc.consumerType,
		"consumer-type", "print", "consumer type")

	productionCmd.Flags().IntVar(&pc.workerCount, "worker-count", 2, "worker count")

	productionCmd.Flags().StringVar(&pc.elasticServer,
		"elasticsearch-server", "", "elasticsearch server")
	productionCmd.Flags().IntVar(&pc.bulkSize, "bulk-size", 20, "bulk size")

	productionCmd.Flags().BoolVar(&pc.isDebug, "debug", true, "debug mode")

	productionCmd.MarkFlagRequired("rabbitmq-server")
	productionCmd.MarkFlagRequired("rabbitmq-queue-name")

	pc.cmd = productionCmd

	return pc
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
