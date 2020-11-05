package main

import (
	"encoding/json"
	"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_client/app"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	"github.com/nwpc-oper/nwpc-message-client/test/client_to_broker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
	"time"
)

func main() {
	Execute()
}

var (
	rabbitmqServer = ""
	workerCount    = 40
	logDirectory   = ""
)

func init() {
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
		"count of worker to send message",
	)

	rootCmd.Flags().StringVar(
		&logDirectory,
		"log-dir",
		"",
		"log director",
	)
	rootCmd.MarkFlagRequired("rabbitmq-server")
	rootCmd.MarkFlagRequired("log-dir")

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.999999",
		FullTimestamp:   true,
	})
}

var rootCmd = &cobra.Command{
	Use:   "test_client_to_rabbitmq",
	Short: "Test client to rabbitmq.",
	Long:  "Test client to rabbitmq.",
	Run: func(cmd *cobra.Command, args []string) {
		for i := 0; i < workerCount; i++ {
			go func(index int) {
				workerLog, logFile := client_to_broker.CreateWorkerLog(index, logDirectory)
				defer logFile.Close()
				c := time.Tick(1 * time.Second)
				for _ = range c {
					SendMessage(
						index,
						workerLog,
					)
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

const (
	exchangeName = "nwpc.operation.workflow.test"
	routeKeyName = "ecflow.command.ecflow_client"
	writeTimeOut = 2 * time.Second
)

func SendMessage(index int, workerLog *log.Logger) {
	data, err := common.CreateEcflowClientMessage("--init=31134")
	message := common.EventMessage{
		App:  "nwpc-message-client",
		Type: app.EcflowClientMessageType,
		Time: time.Now(),
		Data: data,
	}

	messageBytes, _ := json.Marshal(message)
	log.WithFields(log.Fields{
		"index": index,
	}).Infof("sending message...")
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

	err = rabbitSender.SendMessage(messageBytes)
	if err != nil {
		workerLog.WithFields(log.Fields{
			"index": index,
		}).Errorf("sending message...err: %v", err)
		log.WithFields(log.Fields{
			"index": index,
		}).Errorf("sending message...err: %v", err)
		return
	}
	return
}
