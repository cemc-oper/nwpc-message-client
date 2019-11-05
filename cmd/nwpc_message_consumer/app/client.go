package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/consumer"
	"github.com/spf13/cobra"
)

var (
	commandOptions = ""
	rabbitmqServer = ""
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
		fmt.Printf("%s\n", err)
	},
}
