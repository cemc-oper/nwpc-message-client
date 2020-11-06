package app

import (
	"github.com/nwpc-oper/nwpc-message-client/common/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type ecflowClientKafkaCommand struct {
	BaseCommand

	brokerServers []string
	topic         string

	isDebug bool
}

func (c *ecflowClientKafkaCommand) consumerEcflowClient(cmd *cobra.Command, args []string) error {
	log.WithFields(log.Fields{
		"component": "ecflow-client",
		"event":     "consumer",
	}).Info("start to consume...")

	source := consumer.KafkaSource{
		Brokers: c.brokerServers,
		Topic:   c.topic,
	}

	currentConsumer := consumer.KafkaPrinterConsumer{
		Source: source,
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

func newEcflowClientKafkaCommand() *ecflowClientKafkaCommand {
	ec := &ecflowClientKafkaCommand{}

	ecflowClientCmd := &cobra.Command{
		Use:   "ecflow-client-kafka",
		Short: "consume message from ecflow client command",
		Long:  "",
		RunE:  ec.consumerEcflowClient,
	}

	ecflowClientCmd.Flags().StringVar(
		&ec.topic,
		"topic",
		"",
		"topic")

	ecflowClientCmd.Flags().StringSliceVar(
		&ec.brokerServers,
		"brokers",
		[]string{},
		"brokers")

	ec.cmd = ecflowClientCmd

	return ec
}
