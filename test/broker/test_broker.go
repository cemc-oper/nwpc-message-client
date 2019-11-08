package main

import (
	"context"
	"encoding/json"
	"github.com/nwpc-oper/nwpc-message-client/cmd/nwpc_message_client/app"
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"os"
	"time"
)

func main() {
	Execute()
}

var (
	brokerAddress  = ""
	rabbitmqServer = ""
	workerCount    = 40
)

func init() {
	rootCmd.Flags().StringVar(&brokerAddress, "broker-address", ":33383", "broker rpc address")
	rootCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server address")
	rootCmd.Flags().IntVar(&workerCount, "worker-count", 40, "count of worker to send message")

	rootCmd.MarkFlagRequired("broker-address")
	rootCmd.MarkFlagRequired("rabbitmq-server")
}

var rootCmd = &cobra.Command{
	Use:   "test_broker",
	Short: "Test broker.",
	Long:  "Test broker.",
	Run: func(cmd *cobra.Command, args []string) {
		for i := 0; i < workerCount; i++ {
			go func() {
				c := time.Tick(1 * time.Second)
				for _ = range c {
					SendMessage()
				}
			}()
		}
		select {}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func SendMessage() {
	data, err := common.CreateEcflowClientMessage("--init=31134")
	message := app.EventMessage{
		App:  "nwpc-message-client",
		Type: app.EcflowClientMessageType,
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
		}).Errorf("connect to broker has error: %v\n", err)
		return
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
		}).Errorf("send message has error: %v", err)
		return
	}

	if response.ErrorNo != 0 {
		log.WithFields(log.Fields{
			"component": "ecflow-client",
			"event":     "response",
		}).Errorf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
		return
	}
}
