package app

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"time"
)

const EcflowClientMessageType = "ecflow-client"
const ecflowClientDescription = `
Send messages for ecflow_client command. 
Messages are send to a rabbitmq server directly or via a broker running by nwpc_message_client broker command.
`

type ecflowClientCommand struct {
	BaseCommand
	commandOptions string
	useBroker      bool
	rabbitmqServer string
	brokerAddress  string
	disableSend    bool
	writeTimeout   time.Duration
}

func (ec *ecflowClientCommand) runCommand(cmd *cobra.Command, args []string) error {
	data, err := common.CreateEcflowClientMessage(ec.commandOptions)
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

	if ec.disableSend {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "send",
		}).Infof("message deliver is disabled by --disable-send option.")
		return nil
	}

	senderType := RabbitMQSenderType
	if ec.useBroker {
		senderType = BrokerSenderType
	}

	var currentSender sender.Sender
	switch senderType {
	case RabbitMQSenderType:
		currentSender = sender.CreateRabbitMQSender(
			ec.rabbitmqServer, exchangeName, routeKeyName, ec.writeTimeout)
		break
	case BrokerSenderType:
		currentSender = sender.CreateBrokerSender(
			ec.brokerAddress, ec.rabbitmqServer, exchangeName, routeKeyName, ec.writeTimeout)
		break
	default:
		return fmt.Errorf("SenderType is not supported: %d", senderType)
	}

	return sendMessage(currentSender, messageBytes)
}

func newEcflowClientCommand() *ecflowClientCommand {
	ec := &ecflowClientCommand{
		writeTimeout: 2 * time.Second,
	}
	ecFlowClientCmd := &cobra.Command{
		Use:   "ecflow-client",
		Short: "send messages for ecflow_client command",
		Long:  ecflowClientDescription,
		RunE:  ec.runCommand,
	}

	ecFlowClientCmd.Flags().StringVar(&ec.commandOptions, "command-options", "",
		"ecflow_client command options")

	ecFlowClientCmd.Flags().StringVar(&ec.rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")

	ecFlowClientCmd.Flags().BoolVar(&ec.useBroker, "with-broker", true,
		"deliver message using a broker, should set --broker-address when enabled.")
	ecFlowClientCmd.Flags().StringVar(&ec.brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")

	ecFlowClientCmd.Flags().BoolVar(&ec.disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

	ecFlowClientCmd.MarkFlagRequired("command-options")
	ecFlowClientCmd.MarkFlagRequired("rabbitmq-server")

	ec.cmd = ecFlowClientCmd
	return ec
}
