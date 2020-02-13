package sender

import (
	"context"
	"fmt"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	"google.golang.org/grpc"
	"time"
)

type BrokerSender struct {
	BrokerAddress string
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

	client := pb.NewMessageBrokerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
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
		return fmt.Errorf("send message has error: %v", err)
	}

	if response.ErrorNo != 0 {
		return fmt.Errorf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
	}

	return nil
}
