package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const ecflowLongDescription = `
Consume ecflow_client message from rabbitmq and store them into elasticsearch.

The command will use elasticsearch's Bulk API to send multiply messages 
when some count of messages (default 20) are received or time limit (1 second) is reached.

The command run multiply goroutines (like threads, default is 2) to receive messages.
`

type ecflowClientCommand struct {
	BaseCommand

	consumerType string

	rabbitmqServer       string
	rabbitmqQueueName    string
	rabbitmqExchangeName string

	elasticServer string

	workerCount int
	bulkSize    int

	isDebug bool
}

func (c *ecflowClientCommand) consumerEcflowClient(cmd *cobra.Command, args []string) error {
	log.WithFields(log.Fields{
		"component": "ecflow-client",
		"event":     "consumer",
	}).Info("start to consume...")

	source := consumer.RabbitMQSource{
		Server:   c.rabbitmqServer,
		Exchange: c.rabbitmqExchangeName,
		Topics:   []string{"ecflow.command.ecflow_client"},
		Queue:    c.rabbitmqQueueName,
	}

	var currentConsumer consumer.Consumer = nil
	if c.consumerType == string(printerConsumerType) {
		currentConsumer = createPrinterConsumer(
			source,
			c.workerCount,
			c.isDebug)
	} else if c.consumerType == string(elasticsearchConsumerType) {
		target := consumer.ElasticSearchTarget{
			Server: c.elasticServer,
		}
		currentConsumer = createEcflowClientConsumer(
			source,
			target,
			c.workerCount,
			c.bulkSize,
			c.isDebug)
	}

	if currentConsumer == nil {
		log.Fatalf("consumer type is not supported: %s", c.consumerType)
		return fmt.Errorf("consumer type is not supported: %s", c.consumerType)
	}

	err := currentConsumer.ConsumeMessages()
	if err != nil {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "consumer",
		}).Errorf("%v", err)
	}
	return err
}

func newEcflowClientCommand() *ecflowClientCommand {
	ec := &ecflowClientCommand{}

	ecflowClientCmd := &cobra.Command{
		Use:   "ecflow-client",
		Short: "consume message from ecflow client command",
		Long:  ecflowLongDescription,
		RunE:  ec.consumerEcflowClient,
	}

	ecflowClientCmd.Flags().StringVar(
		&ec.consumerType,
		"consumer-type",
		"elasticsearch",
		"consumer type, printer or elasticsearch",
	)

	ecflowClientCmd.Flags().StringVar(
		&ec.rabbitmqServer,
		"rabbitmq-server",
		"",
		"rabbitmq server",
	)
	ecflowClientCmd.Flags().StringVar(
		&ec.rabbitmqQueueName,
		"rabbitmq-queue-name",
		"ecflow-client-queue",
		"rabbitmq queue name",
	)
	ecflowClientCmd.Flags().StringVar(
		&ec.rabbitmqExchangeName,
		"rabbitmq-exchange-name",
		"nwpc.operation.workflow",
		"rabbitmq exchange name",
	)

	ecflowClientCmd.Flags().StringVar(
		&ec.elasticServer,
		"elasticsearch-server",
		"",
		"elasticsearch server",
	)

	ecflowClientCmd.Flags().IntVar(
		&ec.workerCount,
		"worker-count",
		2,
		"worker count",
	)
	ecflowClientCmd.Flags().IntVar(
		&ec.bulkSize,
		"bulk-size",
		20,
		"bulk size",
	)

	ecflowClientCmd.Flags().BoolVar(
		&ec.isDebug,
		"debug",
		true,
		"debug mode",
	)

	ecflowClientCmd.MarkFlagRequired("rabbitmq-server")
	ecflowClientCmd.MarkFlagRequired("rabbitmq-queue-name")
	ecflowClientCmd.MarkFlagRequired("elastic-server")

	ec.cmd = ecflowClientCmd

	return ec
}
