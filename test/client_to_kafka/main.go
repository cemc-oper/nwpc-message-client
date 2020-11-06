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
	brokerServers []string
	workerCount   = 1
	logDirectory  = ""
)

func init() {
	rootCmd.Flags().StringSliceVar(
		&brokerServers,
		"brokers",
		[]string{},
		"brokers",
	)

	rootCmd.Flags().IntVar(
		&workerCount,
		"worker-count",
		1,
		"count of worker to send message",
	)

	rootCmd.Flags().StringVar(
		&logDirectory,
		"log-dir",
		"",
		"log director",
	)
	rootCmd.MarkFlagRequired("brokers")
	rootCmd.MarkFlagRequired("log-dir")

	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.999999",
		FullTimestamp:   true,
	})
}

var rootCmd = &cobra.Command{
	Use:   "test_client_to_kafka",
	Short: "Test client to kafka.",
	Long:  "Test client to kafka.",
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
	topic        = "nwpc-operation-workflow-test"
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
	//log.WithFields(log.Fields{
	//	"index": index,
	//}).Infof("sending message...")
	target := sender.KafkaTarget{
		Brokers:      brokerServers,
		Topic:        topic,
		WriteTimeout: writeTimeOut,
	}

	rabbitSender := sender.KafkaSender{
		Target: target,
		Debug:  true,
	}

	err = rabbitSender.SendMessage(messageBytes)
	if err != nil {
		workerLog.WithFields(log.Fields{
			"index": index,
		}).Errorf("sending message: %v", err)
		log.WithFields(log.Fields{
			"index": index,
		}).Errorf("sending message: %v", err)
		return
	}
	return
}
