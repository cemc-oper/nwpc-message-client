package common

import (
	"context"
	"fmt"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"time"
)

type MessageBrokerServer struct {
	pb.UnimplementedMessageBrokerServer
	DisableDeliver bool
}

func (s *MessageBrokerServer) SendRabbitMQMessage(ctx context.Context, req *pb.RabbitMQMessage) (*pb.Response, error) {
	log.WithFields(log.Fields{
		"component": "broker",
		"event":     "message",
	}).Infof("receiving message...%s\n", req.GetMessage().GetData())
	rabbitmqTarget := sender.RabbitMQTarget{
		Server:       req.GetTarget().GetServer(),
		Exchange:     req.GetTarget().GetExchange(),
		RouteKey:     req.GetTarget().GetRouteKey(),
		WriteTimeout: 2 * time.Second,
	}

	rabbitSender := sender.RabbitMQSender{
		Target: rabbitmqTarget,
		Debug:  true,
	}

	response := &pb.Response{}
	response.ErrorNo = 0

	if !s.DisableDeliver {
		err := rabbitSender.SendMessage(req.GetMessage().GetData())

		if err != nil {
			response.ErrorMessage = fmt.Sprintf("send messge has error: %s", err)
		}
	}

	return response, nil
}
