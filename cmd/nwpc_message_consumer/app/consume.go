package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/consumer"
	"github.com/spf13/cobra"
	"time"
)

var (
	commandOptions = ""
	rabbitmqServer = ""
)

func init() {
	rootCmd.AddCommand(consumerCmd)

	consumerCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server")
}

const EcflowClientMessageType = "ecflow-client"

var consumerCmd = &cobra.Command{
	Use:   "consume",
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

type EventMessage struct {
	App  string      `json:"app"`
	Type string      `json:"type"`
	Time time.Time   `json:"time"`
	Data interface{} `json:"data"`
}

type EcflowClientData struct {
	Command    string              `json:"command"`
	Arguments  []string            `json:"args"`
	Envs       []map[string]string `json:"envs"`
	EcflowHost string              `json:"ecf_host"`
	EcflowPort string              `json:"ecf_port"`
	NodeName   string              `json:"ecf_name"`
	NodeRID    string              `json:"ecf_rid"`
	TryNo      string              `json:"ecf_tryno"`
}
