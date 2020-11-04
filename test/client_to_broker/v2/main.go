package main

import (
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
				workerLog, logFile := client_to_broker.CreateWorkerLog(index, logDirectory)
				defer logFile.Close()

				c := time.Tick(1 * time.Second)
				for _ = range c {
					client_to_broker.SendMessage(
						index,
						brokerAddress,
						rabbitmqServer,
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
