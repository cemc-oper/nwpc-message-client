package app

import (
	"github.com/nwpc-oper/nwpc-message-client/consumer"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(ecflowClientCmd)

	ecflowClientCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server")
}

var ecflowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "consume message from ecflow",
	Long:  "consume message from ecflow",
	Run: func(cmd *cobra.Command, args []string) {
		target := consumer.RabbitMQTarget{
			Server: rabbitmqServer,
		}

		consumer := &consumer.RabbitMQConsumer{
			Target: target,
			Debug:  true,
		}

		err := consumer.ConsumeMessages()
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "message",
		}).Infof("%s", err)
	},
}
