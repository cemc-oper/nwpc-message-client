package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	"time"
)

func sendMessage(server string, exchange string, routeKey string, messageBytes []byte) error {
	var currentSender sender.Sender
	if useBroker {
		currentSender = createBrokerSender(brokerAddress, server, exchange, routeKey, writeTimeOut)
	} else {
		currentSender = createRabbitMQSender(server, exchange, routeKey, writeTimeOut)
	}

	err := currentSender.SendMessage(messageBytes)
	if err != nil {
		return fmt.Errorf("send messge has error: %s", err)
	}

	return nil
}

func createBrokerSender(
	brokerAddress string,
	rabbitMQServer string,
	exchange string,
	routeKey string,
	writeTimeout time.Duration) sender.Sender {
	brokerSender := sender.BrokerSender{
		BrokerAddress:  brokerAddress,
		RabbitMQServer: rabbitMQServer,
		Exchange:       exchange,
		RouteKey:       routeKey,
		WriteTimeout:   writeTimeout,
	}

	return &brokerSender
}

func createRabbitMQSender(
	server string,
	exchange string,
	routeKey string,
	writeTimeout time.Duration) sender.Sender {
	rabbitmqTarget := sender.RabbitMQTarget{
		Server:       server,
		Exchange:     exchange,
		RouteKey:     routeKey,
		WriteTimeout: writeTimeout,
	}

	rabbitSender := sender.RabbitMQSender{
		Target: rabbitmqTarget,
		Debug:  true,
	}

	return &rabbitSender
}
