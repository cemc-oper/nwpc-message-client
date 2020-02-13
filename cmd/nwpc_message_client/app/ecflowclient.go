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

	ecFlowClientCmd.Flags().StringVar(&commandOptions, "command-options", "",
		"ecflow_client command options")

	ecFlowClientCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")

	ecFlowClientCmd.Flags().BoolVar(&useBroker, "with-broker", true,
		"deliver message using a broker, should set --broker-address when enabled.")
	ecFlowClientCmd.Flags().StringVar(&brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")

	ecFlowClientCmd.Flags().BoolVar(&disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

	ecFlowClientCmd.MarkFlagRequired("command-options")
	ecFlowClientCmd.MarkFlagRequired("rabbitmq-server")
}

const EcflowClientMessageType = "ecflow-client"
const ecflowClientDescription = `
Send messages for ecflow_client command. 
Messages are send to a rabbitmq server directly or via a broker running by nwpc_message_client broker command.
`

var ecFlowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "send messages for ecflow_client command",
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

		if disableSend {
			log.WithFields(log.Fields{
				"component": "ecflow-client",
				"event":     "send",
			}).Infof("message deliver is disabled by --disable-send option.")
			return nil
		}

		senderType := RabbitMQSenderType
		if useBroker {
			senderType = BrokerSenderType
		}

		return sendMessage(senderType, rabbitmqServer, exchangeName, routeKeyName, messageBytes)
	},
}
