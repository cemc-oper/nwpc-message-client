package app

import (
	"context"
	"fmt"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(brokerAddress, opts...)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "sender",
			"event":     "connection",
		}).Errorf("connect to broker has error: %v\n", err)
		return fmt.Errorf("connect to broker has error: %v\n", err)
	}

	defer conn.Close()

	client := pb.NewMessageBrokerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	response, err := client.SendRabbitMQMessage(ctx, &pb.RabbitMQMessage{
		Target: &pb.RabbitMQTarget{
			Server:   server,
			Exchange: exchange,
			RouteKey: routeKey,
		},
		Message: &pb.Message{
			Data: messageBytes,
		},
	})

	if err != nil {
		log.WithFields(log.Fields{
			"component": "sender",
			"event":     "send",
		}).Errorf("send message has error: %v", err)
		return fmt.Errorf("send message has error: %v", err)
	}

	if response.ErrorNo != 0 {
		log.WithFields(log.Fields{
			"component": "sender",
			"event":     "response",
		}).Errorf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
		return fmt.Errorf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
	}
	return nil
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
