package app

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/commands"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"time"
)

type targetOptions struct {
	rabbitmqServer string
	writeTimeout   time.Duration

	useBroker     bool
	brokerAddress string

	disableSend bool

	exchangeName string
	routeKeyName string
}

func (t *targetOptions) parseCommandTargetOptions(args []string) error {
	targetFlagSet := pflag.NewFlagSet("targetFlagSet", pflag.ContinueOnError)
	targetFlagSet.ParseErrorsWhitelist.UnknownFlags = true

	targetFlagSet.StringVar(&t.rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")

	targetFlagSet.BoolVar(&t.useBroker, "with-broker", false,
		"deliver message using a broker, should set --broker-address when enabled.")
	targetFlagSet.StringVar(&t.brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")

	targetFlagSet.BoolVar(&t.disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

	targetFlagSet.SetAnnotation("rabbitmq-server", commands.RequiredOption, []string{"true"})

	err := targetFlagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}

	err = commands.CheckRequiredFlags(targetFlagSet)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	return nil
}

func sendToTarget(options targetOptions, message common.EventMessage) error {
	messageBytes, _ := json.Marshal(message)
	messageBytesIndent, _ := json.MarshalIndent(message, "", "  ")
	if options.disableSend {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "send",
		}).Infof("message deliver is disabled by --disable-send option.")
		fmt.Printf("%s\n", messageBytesIndent)
		return nil
	}

	senderType := RabbitMQSenderType
	if options.useBroker {
		senderType = BrokerSenderType
	}

	var currentSender sender.Sender
	switch senderType {
	case RabbitMQSenderType:
		currentSender = sender.CreateRabbitMQSender(
			options.rabbitmqServer, options.exchangeName, options.routeKeyName, options.writeTimeout)
		break
	case BrokerSenderType:
		currentSender = sender.CreateBrokerSender(
			options.brokerAddress,
			options.rabbitmqServer,
			options.exchangeName,
			options.routeKeyName,
			options.writeTimeout)
		break
	default:
		return fmt.Errorf("SenderType is not supported: %d", senderType)
	}
	log.WithFields(log.Fields{
		"component": "message",
		"event":     "print",
	}).Infof("%s", messageBytes)
	fmt.Printf("%s\n", messageBytesIndent)

	return sendMessage(currentSender, messageBytes)
}
