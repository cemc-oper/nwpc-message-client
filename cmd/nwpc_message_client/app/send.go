package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	"time"
)

func sendMessage(server string, exchange string, routeKey string, messageBytes []byte) error {
	var currentSender sender.Sender
	if useBroker {
		currentSender = createBrokerSender(server, exchange, routeKey)
	} else {
		currentSender = createRabbitMQSender(server, exchange, routeKey)
	}

	err := currentSender.SendMessage(messageBytes)
	if err != nil {
		return fmt.Errorf("send messge has error: %s", err)
	}

	return nil
}

func createBrokerSender(server string, exchange string, routeKey string) sender.Sender {
	brokerSender := sender.BrokerSender{
		BrokerAddress:  brokerAddress,
		RabbitMQServer: server,
		Exchange:       exchange,
		RouteKey:       routeKey,
		WriteTimeout:   time.Second * 2,
	}

	return &brokerSender
}

func createRabbitMQSender(server string, exchange string, routeKey string) sender.Sender {
	rabbitmqTarget := sender.RabbitMQTarget{
		Server:       server,
		Exchange:     exchange,
		RouteKey:     routeKey,
		WriteTimeout: writeTimeOut,
	}

	rabbitSender := sender.RabbitMQSender{
		Target: rabbitmqTarget,
		Debug:  true,
	}

	return &rabbitSender
}
