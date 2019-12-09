package app

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"time"
)

func init() {
	rootCmd.AddCommand(productionCmd)

	productionCmd.Flags().StringVar(&system, "system", "", "system")
	productionCmd.Flags().StringVar(&productionType, "production-type", "", "production type")
	productionCmd.Flags().StringVar(&event, "event", "",
		"production event, such as storage")
	productionCmd.Flags().StringVar(&status, "status", "completed",
		"event status, such as completed, aborted.")
	productionCmd.Flags().StringVar(&startTime, "start-time", "", "start time, YYYYMMDDHH")
	productionCmd.Flags().StringVar(&forecastTime, "forecast-time", "",
		"forecast time, FFFh, 0h, 12h, ...")

	productionCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "", "rabbitmq server")
	productionCmd.Flags().BoolVar(&useBroker, "with-broker", true, "use a broker")
	productionCmd.Flags().StringVar(&brokerAddress, "broker-address", "", "broker address")

	productionCmd.MarkFlagRequired("system")
	productionCmd.MarkFlagRequired("production-type")
	productionCmd.MarkFlagRequired("event")
	productionCmd.MarkFlagRequired("start-time")
	productionCmd.MarkFlagRequired("forecast-time")

	productionCmd.MarkFlagRequired("rabbitmq-server")
}

const ProductionMessageType = "production"
const productionDescription = `
Send messages for production.
Messages are send to a rabbitmq server via a broker running by broker command.
`

var productionCmd = &cobra.Command{
	Use:   "production",
	Short: "send production message to broker",
	Long:  productionDescription,
	RunE: func(cmd *cobra.Command, args []string) error {
		data := common.ProductionData{
			System:       system,
			Type:         productionType,
			Event:        event,
			Status:       status,
			StartTime:    startTime,
			ForecastTime: forecastTime,
		}

		message := common.EventMessage{
			App:  appName,
			Type: ProductionMessageType,
			Time: time.Now(),
			Data: data,
		}

		messageBytes, _ := json.Marshal(message)

		log.WithFields(log.Fields{
			"component": "production",
			"event":     "message",
		}).Infof("%s", messageBytes)

		exchangeName := "nwpc.operation.production"
		routeKeyName := fmt.Sprintf("%s.production.%s", system, productionType)

		return sendMessage(rabbitmqServer, exchangeName, routeKeyName, messageBytes)
	},
}
