package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
)

type SenderType int

const (
	RabbitMQSenderType SenderType = iota
	BrokerSenderType
)

func sendMessage(senderType SenderType, server string, exchange string, routeKey string, messageBytes []byte) error {
	var currentSender sender.Sender
	switch senderType {
	case RabbitMQSenderType:
		currentSender = sender.CreateRabbitMQSender(server, exchange, routeKey, writeTimeOut)
		break
	case BrokerSenderType:
		currentSender = sender.CreateBrokerSender(brokerAddress, server, exchange, routeKey, writeTimeOut)
		break
	default:
		return fmt.Errorf("SenderType is not supported: %d", senderType)
	}

	err := currentSender.SendMessage(messageBytes)
	if err != nil {
		return fmt.Errorf("send messge has error: %s", err)
	}

	return nil
}
