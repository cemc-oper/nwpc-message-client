package app

import (
	"context"
	"encoding/json"
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"time"
)

func init() {
	rootCmd.AddCommand(ecFlowClientCmd)

	ecFlowClientCmd.Flags().StringVar(&commandOptions, "command-options", "", "command options")
	ecFlowClientCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server")
	ecFlowClientCmd.Flags().StringVar(&brokerAddress, "broker-address", "", "broker address")

	rootCmd.MarkFlagRequired("command-options")
	rootCmd.MarkFlagRequired("rabbitmq-server")
	rootCmd.MarkFlagRequired("broker-address")
}

const EcflowClientMessageType = "ecflow-client"
const ecflowClientDescription = `
Send messages for ecflow_client command.
Messages are send to a rabbitmq server via a broker running by ecflow_client broker command.
`

var ecFlowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "send ecflow_client message to broker",
	Long:  ecflowClientDescription,
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

		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "message",
		}).Infof("%s", messageBytes)

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		conn, err := grpc.Dial(brokerAddress, opts...)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "ecflow-client",
				"event":     "connection",
			}).Fatalf("connect to broker has error: %v\n", err)
		}

		defer conn.Close()

		client := pb.NewMessageBrokerClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
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
			log.WithFields(log.Fields{
				"component": "ecflow-client",
				"event":     "send",
			}).Fatalf("send message has error: %v", err)
		}

		if response.ErrorNo != 0 {
			log.WithFields(log.Fields{
				"component": "ecflow-client",
				"event":     "response",
			}).Fatalf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
		}
	},
}

type EventMessage struct {
	App  string      `json:"app"`
	Type string      `json:"type"`
	Time time.Time   `json:"time"`
	Data interface{} `json:"data"`
}
