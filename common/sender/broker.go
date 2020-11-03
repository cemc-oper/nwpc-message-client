package sender

import (
	"context"
	"fmt"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"time"
)

type BrokerSender struct {
	BrokerAddress string
	BrokerTryNo   int
	Target        RabbitMQTarget
}

func (s *BrokerSender) SendMessage(message []byte) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(s.BrokerAddress, opts...)
	if err != nil {
		return fmt.Errorf("connect to broker has error: %v\n", err)
	}

	defer conn.Close()

	currentCount := 0
	totalCount := 2
	if s.BrokerTryNo == 0 {
		totalCount = 1
	}

	successful := false
	for currentCount < totalCount {
		currentCount += 1
		timeLimit := time.Second * time.Duration(1+currentCount)
		client := pb.NewMessageBrokerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), timeLimit)
		defer cancel()

		response, err := client.SendRabbitMQMessage(ctx, &pb.RabbitMQMessage{
			Target: &pb.RabbitMQTarget{
				Server:   s.Target.Server,
				Exchange: s.Target.Exchange,
				RouteKey: s.Target.RouteKey,
			},
			Message: &pb.Message{
				Data: message,
			},
		})

		if err != nil {
			log.WithFields(log.Fields{
				"component": "sender-broker",
				"event":     "send",
			}).Warningf("send message has error... try %d: %v", currentCount, err)
			continue
		}

		if response.ErrorNo != 0 {
			log.WithFields(log.Fields{
				"component": "sender-broker",
				"event":     "send",
			}).Warningf("send message return error code... try %d:  %d: %s",
				currentCount, response.ErrorNo, response.ErrorMessage)
			continue
		}
		successful = true
		break
	}

	if !successful {
		return fmt.Errorf("send message has error after %d tries", totalCount)
	}

	return nil
}
