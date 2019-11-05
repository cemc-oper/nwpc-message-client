package app

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/sender"
	"github.com/spf13/cobra"
	"log"
	"time"
)

var (
	commandOptions = ""
	rabbitmqServer = ""
)

func init() {
	rootCmd.AddCommand(ecFlowClientCmd)

	ecFlowClientCmd.Flags().StringVar(&commandOptions, "common-options", "", "common options")
	ecFlowClientCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server")
}

const EcflowClientMessageType = "ecflow-client"

var ecFlowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "send message for ecflow",
	Long:  "send message for ecflow",
	Run: func(cmd *cobra.Command, args []string) {
		data, err := common.CreateEcflowClientMessage(commandOptions)
		if err != nil {
			log.Fatal(err)
		}

		message := EventMessage{
			App:  "nwpc-message-client",
			Type: EcflowClientMessageType,
			Time: time.Now(),
			Data: data,
		}

		messageBytes, _ := json.Marshal(message)

		fmt.Printf("%s\n", messageBytes)

		// send message
		rabbitmqTarget := sender.RabbitMQTarget{
			Server:       rabbitmqServer,
			WriteTimeout: 2 * time.Second,
		}

		sender := sender.RabbitMQSender{
			Target: rabbitmqTarget,
			Debug:  true,
		}

		err = sender.SendMessage(messageBytes)

		if err != nil {
			log.Fatalf("send messge has error: %s", err)
		}
	},
}

type EventMessage struct {
	App  string      `json:"app"`
	Type string      `json:"type"`
	Time time.Time   `json:"time"`
	Data interface{} `json:"data"`
}
