package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"time"
)

func init() {
	rootCmd.AddCommand(ecFlowClientCmd)

	ecFlowClientCmd.Flags().StringVar(&commandOptions, "command-options", "", "command options")
	ecFlowClientCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server")
	ecFlowClientCmd.Flags().BoolVar(&useBroker, "with-broker", true, "use a broker")
	ecFlowClientCmd.Flags().StringVar(&brokerAddress, "broker-address", "", "broker address")

	rootCmd.MarkFlagRequired("command-options")
	rootCmd.MarkFlagRequired("rabbitmq-server")
}

const EcflowClientMessageType = "ecflow-client"
const ecflowClientDescription = `
Send messages for ecflow_client command.
Messages are send to a rabbitmq server via a broker running by ecflow_client broker command.
`

const (
	exchangeName = "nwpc.operation.workflow"
	routeKeyName = "ecflow.command.ecflow_client"
	writeTimeOut = 2 * time.Second
)

var ecFlowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "send ecflow_client message to broker",
	Long:  ecflowClientDescription,
	RunE: func(cmd *cobra.Command, args []string) error {
		data, err := common.CreateEcflowClientMessage(commandOptions)
		if err != nil {
			log.Fatal(err)
		}

		message := common.EventMessage{
			App:  "nwpc-message-client",
			Type: EcflowClientMessageType,
			Time: time.Now(),
			Data: data,
		}

		messageBytes, _ := json.Marshal(message)

		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "message",
		}).Infof("%s", messageBytes)

		if useBroker {
			return sendMessageWithBroker(messageBytes)
		} else {
			return sendMessage(messageBytes)
		}
	},
}

func sendMessageWithBroker(messageBytes []byte) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(brokerAddress, opts...)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
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
			Server:   rabbitmqServer,
			Exchange: exchangeName,
			RouteKey: routeKeyName,
		},
		Message: &pb.Message{
			Data: messageBytes,
		},
	})

	if err != nil {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "send",
		}).Errorf("send message has error: %v", err)
		return fmt.Errorf("send message has error: %v", err)
	}

	if response.ErrorNo != 0 {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "response",
		}).Errorf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
		return fmt.Errorf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
	}
	return nil
}

func sendMessage(messageBytes []byte) error {
	rabbitmqTarget := sender.RabbitMQTarget{
		Server:       rabbitmqServer,
		Exchange:     exchangeName,
		RouteKey:     routeKeyName,
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
