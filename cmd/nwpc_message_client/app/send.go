package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
)

func sendMessage(server string, exchange string, routeKey string, messageBytes []byte) error {
	var currentSender sender.Sender
	if useBroker {
		currentSender = sender.CreateBrokerSender(brokerAddress, server, exchange, routeKey, writeTimeOut)
	} else {
		currentSender = sender.CreateRabbitMQSender(server, exchange, routeKey, writeTimeOut)
	}

	err := currentSender.SendMessage(messageBytes)
	if err != nil {
		return fmt.Errorf("send messge has error: %s", err)
	}

	return nil
}
