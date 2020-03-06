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

func sendMessage(currentSender sender.Sender, messageBytes []byte) error {
	err := currentSender.SendMessage(messageBytes)
	if err != nil {
		return fmt.Errorf("send messge has error: %s", err)
	}

	return nil
}
