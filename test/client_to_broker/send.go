package client_to_broker

import (
	"encoding/json"
	"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_client/app"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

func SendMessage(
	index int,
	broker string,
	rabbitmq string,
	workerLog *log.Logger,
) {
	brokerSender := sender.BrokerSender{
		BrokerAddress: broker,
		BrokerTryNo:   2,
		Target: sender.RabbitMQTarget{
			Server:       rabbitmq,
			Exchange:     "nwpc-message",
			RouteKey:     "command.ecflow.ecflow_client",
			WriteTimeout: time.Second,
		},
	}

	data, _ := common.CreateEcflowClientMessage("--init=31134")
	message := common.EventMessage{
		App:  "nwpc-message-client",
		Type: app.EcflowClientMessageType,
		Time: time.Now(),
		Data: data,
	}

	messageBytes, _ := json.Marshal(message)
	err := brokerSender.SendMessage(messageBytes)
	if err != nil {
		workerLog.WithFields(log.Fields{
			"index": index,
			"event": "error",
		}).Errorf("send message failed: %v", err)
		log.WithFields(log.Fields{
			"index": index,
			"event": "error",
		}).Errorf("send message failed: %v", err)
	}

	return
}

func SendMessageViaBrokers(
	index int,
	brokers []string,
	rabbitmq string,
	workerLog *log.Logger,
) {
	var err error
	brokerCount := len(brokers)

	currentCount := 0
	totalCount := 2

	successful := false

	for currentCount < totalCount {
		currentCount += 1

		brokerIndex := rand.Intn(brokerCount)
		broker := brokers[brokerIndex]

		brokerSender := sender.BrokerSender{
			BrokerAddress: broker,
			BrokerTryNo:   2,
			Target: sender.RabbitMQTarget{
				Server:       rabbitmq,
				Exchange:     "nwpc-message",
				RouteKey:     "command.ecflow.ecflow_client",
				WriteTimeout: time.Second,
			},
		}

		data, _ := common.CreateEcflowClientMessage("--init=31134")
		message := common.EventMessage{
			App:  "nwpc-message-client",
			Type: app.EcflowClientMessageType,
			Time: time.Now(),
			Data: data,
		}

		messageBytes, _ := json.Marshal(message)
		err = brokerSender.SendMessage(messageBytes)
		if err != nil {
			continue
		} else {
			successful = true
			break
		}
	}

	if !successful {
		workerLog.WithFields(log.Fields{
			"index": index,
			"event": "error",
		}).Errorf("send message failed: %v", err)
		log.WithFields(log.Fields{
			"index": index,
			"event": "error",
		}).Errorf("send message failed: %v", err)
	}
}
