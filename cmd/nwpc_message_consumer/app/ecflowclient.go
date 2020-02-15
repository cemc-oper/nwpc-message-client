package app

import (
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
	cmd               *cobra.Command
	rabbitmqServer    string
	rabbitmqQueueName string
	elasticServer     string
	workerCount       int
	bulkSize          int
	isDebug           bool
	consumerType      string
}

func (c *ecflowClientCommand) getCommand() *cobra.Command {
	return c.cmd
}

func (c *ecflowClientCommand) consumerEcflowClient(cmd *cobra.Command, args []string) error {
	log.WithFields(log.Fields{
		"component": "ecflow-client",
		"event":     "consumer",
	}).Info("start to consume...")

	ecConsumer := &consumer.EcflowClientConsumer{
		Source: consumer.RabbitMQSource{
			Server:   c.rabbitmqServer,
			Exchange: "nwpc.operation.workflow",
			Topics:   []string{"ecflow.command.ecflow_client"},
			Queue:    c.rabbitmqQueueName,
		},
		Target: consumer.ElasticSearchTarget{
			Server: c.elasticServer,
		},
		WorkerCount: c.workerCount,
		BulkSize:    c.bulkSize,
		Debug:       c.isDebug,
	}

	err := ecConsumer.ConsumeMessages()
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

	ecflowClientCmd.Flags().StringVar(&ec.rabbitmqServer,
		"rabbitmq-server", "", "rabbitmq server")
	ecflowClientCmd.Flags().StringVar(&ec.rabbitmqQueueName,
		"rabbitmq-queue-name", "ecflow-client-queue", "rabbitmq queue name")
	ecflowClientCmd.Flags().StringVar(&ec.elasticServer,
		"elasticsearch-server", "", "elasticsearch server")
	ecflowClientCmd.Flags().IntVar(&ec.workerCount, "worker-count", 2, "worker count")
	ecflowClientCmd.Flags().IntVar(&ec.bulkSize, "bulk-size", 20, "bulk size")
	ecflowClientCmd.Flags().BoolVar(&ec.isDebug, "debug", true, "debug mode")

	ecflowClientCmd.MarkFlagRequired("rabbitmq-server")
	ecflowClientCmd.MarkFlagRequired("rabbitmq-queue-name")
	ecflowClientCmd.MarkFlagRequired("elastic-server")

	ec.cmd = ecflowClientCmd

	return ec
}
