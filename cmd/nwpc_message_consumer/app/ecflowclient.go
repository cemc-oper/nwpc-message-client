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
	ecflowClientCmd.Flags().StringVar(&elasticServer,
		"elasticsearch-server", "", "elasticsearch server")

	rootCmd.MarkFlagRequired("rabbitmq-server")
	rootCmd.MarkFlagRequired("elastic-server")
}

var ecflowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "consume message from ecflow client command",
	Long:  "consume message from ecflow client command",
	RunE: func(cmd *cobra.Command, args []string) error {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "consumer",
		}).Info("start to consume...")

		consumer := &consumer.EcflowClientConsumer{
			Source: consumer.RabbitMQSource{
				Server: rabbitmqServer,
			},
			Target: consumer.ElasticSearchTarget{
				Server: elasticServer,
			},
			Debug: true,
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
