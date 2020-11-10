package common

import (
	"context"
	"fmt"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	"time"
)

type MessageBrokerServer struct {
	pb.MessageBrokerServer
	DisableDeliver bool
	BrokerMode     string
	MessageChan    chan RabbitMQMessage
}

type RabbitMQMessage struct {
	Target  sender.RabbitMQTarget
	Message []byte
}

func (s *MessageBrokerServer) SendRabbitMQMessage(ctx context.Context, req *pb.RabbitMQMessage) (*pb.Response, error) {
	//log.WithFields(log.Fields{
	//	"component": "broker",
	//	"event":     "message",
	//}).Infof("receiving message...%s\n", req.GetMessage().GetData())
	if s.BrokerMode == "batch" {
		rabbitmqTarget := sender.RabbitMQTarget{
			Server:       req.GetTarget().GetServer(),
			Exchange:     req.GetTarget().GetExchange(),
			RouteKey:     req.GetTarget().GetRouteKey(),
			WriteTimeout: 2 * time.Second,
		}
		m := RabbitMQMessage{
			Target:  rabbitmqTarget,
			Message: req.GetMessage().GetData(),
		}
		s.MessageChan <- m

		response := &pb.Response{}
		response.ErrorNo = 0
		return response, nil
	} else {
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
}

func (s *MessageBrokerServer) SendKafkaMessage(ctx context.Context, req *pb.KafkaMessage) (*pb.Response, error) {
	response := &pb.Response{}
	response.ErrorNo = 0
	return response, nil
}
