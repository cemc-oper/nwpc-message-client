package app

import (
	"github.com/nwpc-oper/nwpc-message-client/common/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const predictLongDescription = `
Consumer predict message.
`

type predictCommand struct {
	BaseCommand

	consumerType string

	rabbitmqServer    string
	rabbitmqQueueName string

	elasticServer string

	workerCount int
	bulkSize    int

	isDebug bool
}

func (c *predictCommand) consumerPredict(cmd *cobra.Command, args []string) error {
	log.WithFields(log.Fields{
		"component": "predict",
		"event":     "consumer",
	}).Info("start to consume...")

	source := consumer.RabbitMQSource{
		Server:   c.rabbitmqServer,
		Exchange: "nwpc.operation.predict",
		Topics:   []string{"*.predict.*"},
		Queue:    c.rabbitmqQueueName,
	}

	target := consumer.ElasticSearchTarget{
		Server: c.elasticServer,
	}

	predictComsumer := createPredictConsumer(
		source,
		target,
		c.workerCount,
		c.bulkSize,
		c.isDebug,
	)

	err := predictComsumer.ConsumeMessages()
	if err != nil {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "consumer",
		}).Errorf("%v", err)
	}
	return err
}

func newPredictCommand() *predictCommand {
	ec := &predictCommand{}

	predictCmd := &cobra.Command{
		Use:   "predict",
		Short: "consume predict message",
		Long:  predictLongDescription,
		RunE:  ec.consumerPredict,
	}

	predictCmd.Flags().StringVar(&ec.rabbitmqServer,
		"rabbitmq-server", "", "rabbitmq server")
	predictCmd.Flags().StringVar(&ec.rabbitmqQueueName,
		"rabbitmq-queue-name", "ecflow-client-queue", "rabbitmq queue name")
	predictCmd.Flags().StringVar(&ec.elasticServer,
		"elasticsearch-server", "", "elasticsearch server")
	predictCmd.Flags().IntVar(&ec.workerCount, "worker-count", 2, "worker count")
	predictCmd.Flags().IntVar(&ec.bulkSize, "bulk-size", 20, "bulk size")
	predictCmd.Flags().BoolVar(&ec.isDebug, "debug", true, "debug mode")

	predictCmd.MarkFlagRequired("rabbitmq-server")
	predictCmd.MarkFlagRequired("rabbitmq-queue-name")
	predictCmd.MarkFlagRequired("elastic-server")

	ec.cmd = predictCmd

	return ec
}
