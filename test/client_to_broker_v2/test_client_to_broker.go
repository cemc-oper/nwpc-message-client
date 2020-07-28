package main

import (
	"encoding/json"
	"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_client/app"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
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

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.999999",
		FullTimestamp:   true,
	})
}

var rootCmd = &cobra.Command{
	Use:   "test_broker",
	Short: "Test broker.",
	Long:  "Test broker.",
	Run: func(cmd *cobra.Command, args []string) {
		for i := 0; i < workerCount; i++ {
			go func(index int) {
				c := time.Tick(1 * time.Second)
				for _ = range c {
					SendMessage(index)
				}
			}(i)
		}
		select {}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func SendMessage(index int) {

	brokerSender := sender.BrokerSender{
		BrokerAddress: brokerAddress,
		BrokerTryNo:   2,
		Target: sender.RabbitMQTarget{
			Server:       rabbitmqServer,
			Exchange:     "nwpc-message",
			RouteKey:     "command.ecflow.ecflow_client",
			WriteTimeout: time.Second,
		},
	}

	data, _ := common.CreateEcflowClientMessage("--init=31134")
	message := common.EventMessage{
		App:  "nwpc-message-client",
		Type: app.EcflowClientMessageType,
		Time: time.Now(),
		Data: data,
	}

	messageBytes, _ := json.Marshal(message)
	err := brokerSender.SendMessage(messageBytes)
	if err != nil {
		log.WithFields(log.Fields{
			"index": index,
			"event": "error",
		}).Errorf("send message has error: %v", err)
	}

	return
}
