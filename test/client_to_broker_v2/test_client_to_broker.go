package main

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_client/app"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"time"
)

func main() {
	Execute()
}

var (
	brokerAddress  = ""
	rabbitmqServer = ""
	logDirectory   = ""
	workerCount    = 40
)

func init() {
	rootCmd.Flags().StringVar(
		&brokerAddress,
		"broker-address",
		":33383",
		"broker rpc address, run by `nwpc_message_client broker` command.",
	)
	rootCmd.Flags().StringVar(
		&rabbitmqServer,
		"rabbitmq-server",
		"",
		"rabbitmq server address",
	)
	rootCmd.Flags().IntVar(
		&workerCount,
		"worker-count",
		40,
		"count of workers to send message",
	)
	rootCmd.Flags().StringVar(
		&logDirectory,
		"log-dir",
		"",
		"log director",
	)

	rootCmd.MarkFlagRequired("log-dir")
	rootCmd.MarkFlagRequired("broker-address")
	rootCmd.MarkFlagRequired("rabbitmq-server")

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.999999",
		FullTimestamp:   true,
	})
}

const LongDescription = `
Test client send message to rabbitmq via broker.

Client will try to send message again if first try fails.

Each worker send one message every second.
`

var rootCmd = &cobra.Command{
	Use:   "client_to_broker_v2",
	Short: "Test client to broker.",
	Long:  LongDescription,
	Run: func(cmd *cobra.Command, args []string) {
		_ = os.MkdirAll(logDirectory, 0755)

		for i := 0; i < workerCount; i++ {
			go func(index int) {
				var workerLog = log.New()

				logName := fmt.Sprintf("worker.%d.log", index)
				logPath := filepath.Join(logDirectory, logName)
				file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
				defer file.Close()
				if err == nil {
					workerLog.SetOutput(file)
				} else {
					workerLog.Fatal("Failed to log to file, using default stderr: %v", err)
				}

				c := time.Tick(1 * time.Second)
				for _ = range c {
					SendMessage(index, workerLog)
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

func SendMessage(index int, workerLog *log.Logger) {
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
		workerLog.WithFields(log.Fields{
			"index": index,
			"event": "error",
		}).Errorf("send message failed: %v", err)
	}

	return
}
