package common

import (
	"context"
	"fmt"
	pb "github.com/nwpc-oper/nwpc-message-client/messagebroker"
	"github.com/nwpc-oper/nwpc-message-client/sender"
	"log"
	"time"
)

type MessageBrokerServer struct {
	pb.UnimplementedMessageBrokerServer
}

func (s *MessageBrokerServer) SendRabbitMQMessage(ctx context.Context, req *pb.RabbitMQMessage) (*pb.Response, error) {
	log.Printf("receiving message...\n")
	rabbitmqTarget := sender.RabbitMQTarget{
		Server:       req.GetTarget().GetServer(),
		WriteTimeout: 2 * time.Second,
	}

	rabbitSender := sender.RabbitMQSender{
		Target: rabbitmqTarget,
		Debug:  true,
	}

	response := &pb.Response{}
	response.ErrorNo = 0

	err := rabbitSender.SendMessage(req.GetMessage().GetData())

	if err != nil {
		response.ErrorMessage = fmt.Sprintf("send messge has error: %s", err)
	}

	return response, nil
}
