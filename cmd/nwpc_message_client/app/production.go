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

	productionCmd.Flags().StringVar(&system, "system", "",
		"system name, such as grapes_gfs_gmf")
	productionCmd.Flags().StringVar(&productionType, "production-type", "",
		fmt.Sprintf("production type, such as %s", common.ProductionTypeGrib2))
	productionCmd.Flags().StringVar(&event, "event", "",
		fmt.Sprintf("production event, such as %s", common.ProductionEventStorage))
	productionCmd.Flags().StringVar(&status, "status", string(common.Complete),
		fmt.Sprintf("event status, such as %s, %s", common.Complete, common.Aborted))

	productionCmd.Flags().StringVar(&startTime, "start-time", "",
		"start time, YYYYMMDDHH")
	productionCmd.Flags().StringVar(&forecastTime, "forecast-time", "",
		"forecast time, FFFh, 0h, 12h, ...")

	productionCmd.Flags().StringVar(&rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")

	productionCmd.Flags().BoolVar(&useBroker, "with-broker", true,
		"deliver message using a broker, should set --broker-address when enabled.")
	productionCmd.Flags().StringVar(&brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")

	productionCmd.Flags().BoolVar(&disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

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
Messages are send to a rabbitmq server directly or via a broker running by broker command.
`

var productionCmd = &cobra.Command{
	Use:   "production",
	Short: "send production messages",
	Long:  productionDescription,
	RunE: func(cmd *cobra.Command, args []string) error {
		data := common.ProductionData{
			System:       system,
			Type:         common.ProductionType(productionType),
			Event:        common.ProductionEvent(event),
			Status:       common.ToEventStatus(status),
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
		fmt.Printf("%s\n", messageBytes)

		exchangeName := "nwpc.operation.production"
		routeKeyName := fmt.Sprintf("%s.production.%s", system, productionType)

		if disableSend {
			log.WithFields(log.Fields{
				"component": "production",
				"event":     "send",
			}).Infof("message deliver is disabled by --disable-send option.")
			return nil
		}

		senderType := RabbitMQSenderType
		if useBroker {
			senderType = BrokerSenderType
		}

		return sendMessage(senderType, rabbitmqServer, exchangeName, routeKeyName, messageBytes)
	},
}
