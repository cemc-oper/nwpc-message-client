package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/messagebroker"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"log"
	"time"
)

var (
	commandOptions = ""
	rabbitmqServer = ""
)

func init() {
	rootCmd.AddCommand(ecFlowClientCmd)

	ecFlowClientCmd.Flags().StringVar(&commandOptions, "command-options", "", "command options")
	ecFlowClientCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server")
	ecFlowClientCmd.Flags().StringVar(&brokerAddress, "broker-address", "", "broker address")
}

const EcflowClientMessageType = "ecflow-client"

var ecFlowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "send message for ecflow",
	Long:  "send message for ecflow",
	Run: func(cmd *cobra.Command, args []string) {
		data, err := common.CreateEcflowClientMessage(commandOptions)
		if err != nil {
			log.Fatal(err)
		}

		message := EventMessage{
			App:  "nwpc-message-client",
			Type: EcflowClientMessageType,
			Time: time.Now(),
			Data: data,
		}

		messageBytes, _ := json.Marshal(message)

		fmt.Printf("%s\n", messageBytes)

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		conn, err := grpc.Dial(brokerAddress, opts...)
		if err != nil {
			log.Fatalf("connect to broker has error: %v\n", err)
		}

		defer conn.Close()

		client := pb.NewMessageBrokerClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		response, err := client.SendRabbitMQMessage(ctx, &pb.RabbitMQMessage{
			Target: &pb.RabbitMQTarget{
				Server:   rabbitmqServer,
				Exchange: "ecflow-client",
				RouteKey: "",
			},
			Message: &pb.Message{
				Data: messageBytes,
			},
		})

		if err != nil {
			log.Fatalf("send message has error: %v", err)
		}

		if response.ErrorNo != 0 {
			log.Fatalf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
		}
	},
}

type EventMessage struct {
	App  string      `json:"app"`
	Type string      `json:"type"`
	Time time.Time   `json:"time"`
	Data interface{} `json:"data"`
}
