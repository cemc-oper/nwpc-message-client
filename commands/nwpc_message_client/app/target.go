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
	brokerTries   int

	disableSend bool

	exchangeName string
	routeKeyName string
}

type targetParser struct {
	option        targetOptions
	defaultOption targetOptions
}

func (t *targetParser) parseCommandTargetOptions(args []string) error {
	targetFlagSet := t.generateFlags()
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

func (t *targetParser) generateFlags() *pflag.FlagSet {
	targetFlagSet := pflag.NewFlagSet("targetFlagSet", pflag.ContinueOnError)
	targetFlagSet.SortFlags = false
	targetFlagSet.ParseErrorsWhitelist.UnknownFlags = true

	targetFlagSet.StringVar(&t.option.rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")
	targetFlagSet.StringVar(&t.option.exchangeName, "exchange-name", t.defaultOption.exchangeName,
		"exchange name for RabbitMQ.")
	targetFlagSet.StringVar(&t.option.routeKeyName, "route-key-name", t.defaultOption.routeKeyName,
		"route key name for RabbitMQ.")

	targetFlagSet.BoolVar(&t.option.useBroker, "with-broker", false,
		"deliver message using a broker, should set --broker-address when enabled.")
	targetFlagSet.StringVar(&t.option.brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")
	targetFlagSet.IntVar(&t.option.brokerTries, "broker-tries", t.defaultOption.brokerTries,
		"try counts when send message to broker, work with --with-broker")

	targetFlagSet.BoolVar(&t.option.disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

	targetFlagSet.SetAnnotation("rabbitmq-server", commands.RequiredOption, []string{"true"})
	return targetFlagSet
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
			options.rabbitmqServer,
			options.exchangeName,
			options.routeKeyName,
			options.writeTimeout)
		break
	case BrokerSenderType:
		currentSender = sender.CreateBrokerSender(
			options.brokerAddress,
			options.brokerTries,
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

func sendBytesToTarget(options targetOptions, messageBytes []byte) error {
	if options.disableSend {
		log.WithFields(log.Fields{
			"component": "nwpc_message_client",
			"event":     "send",
		}).Infof("message deliver is disabled by --disable-send option.")
		fmt.Printf("%s\n", messageBytes)
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
			options.brokerTries,
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
	fmt.Printf("%s\n", messageBytes)

	return sendMessage(currentSender, messageBytes)
}
