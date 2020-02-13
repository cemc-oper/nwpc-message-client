package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"time"
)

func sendMessage(server string, exchange string, routeKey string, messageBytes []byte) error {
	if useBroker {
		return sendMessageToBroker(
			server,
			exchange,
			routeKey,
			messageBytes)
	} else {
		return sendMessageToRabbitmq(
			server,
			exchange,
			routeKey,
			messageBytes)
	}
}

func sendMessageToBroker(server string, exchange string, routeKey string, messageBytes []byte) error {
	brokerSender := sender.BrokerSender{
		BrokerAddress:  brokerAddress,
		RabbitMQServer: server,
		Exchange:       exchange,
		RouteKey:       routeKey,
		WriteTimeout:   time.Second * 2,
	}

	err := brokerSender.SendMessage(messageBytes)

	if err != nil {
		log.WithFields(log.Fields{
			"component": "sender",
			"event":     "connection",
		}).Errorf("send message has error: %v\n", err)
	}

	return err
}

func sendMessageToRabbitmq(server string, exchange string, routeKey string, messageBytes []byte) error {
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

	if !disableDeliver {
		err := rabbitSender.SendMessage(messageBytes)
		if err != nil {
			return fmt.Errorf("send messge has error: %s", err)
		}
	}

	return nil
}
