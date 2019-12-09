package app

import (
	"encoding/json"
	"github.com/nwpc-oper/nwpc-message-client/common"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"time"
)

func init() {
	rootCmd.AddCommand(ecFlowClientCmd)

	ecFlowClientCmd.Flags().StringVar(&commandOptions, "command-options", "", "command options")
	ecFlowClientCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server")
	ecFlowClientCmd.Flags().BoolVar(&useBroker, "with-broker", true, "use a broker")
	ecFlowClientCmd.Flags().StringVar(&brokerAddress, "broker-address", "", "broker address")

	ecFlowClientCmd.MarkFlagRequired("command-options")
	ecFlowClientCmd.MarkFlagRequired("rabbitmq-server")
}

const EcflowClientMessageType = "ecflow-client"
const ecflowClientDescription = `
Send messages for ecflow_client command.
Messages are send to a rabbitmq server via a broker running by ecflow_client broker command.
`

var ecFlowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "send ecflow_client message to broker",
	Long:  ecflowClientDescription,
	RunE: func(cmd *cobra.Command, args []string) error {
		data, err := common.CreateEcflowClientMessage(commandOptions)
		if err != nil {
			log.Fatal(err)
		}

		message := common.EventMessage{
			App:  appName,
			Type: EcflowClientMessageType,
			Time: time.Now(),
			Data: data,
		}

		messageBytes, _ := json.Marshal(message)

		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "message",
		}).Infof("%s", messageBytes)

		exchangeName := "nwpc.operation.workflow"
		routeKeyName := "ecflow.command.ecflow_client"

		return sendMessage(rabbitmqServer, exchangeName, routeKeyName, messageBytes)
	},
}
