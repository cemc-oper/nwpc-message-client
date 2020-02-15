package app

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"time"
)

const ProductionMessageType = "production"
const productionDescription = `
Send messages for production.
Messages are send to a rabbitmq server directly or via a broker running by broker command.
`

type productionCommand struct {
	BaseCommand
	system         string
	productionType string
	event          string
	status         string
	startTime      string
	forecastTime   string
	useBroker      bool
	brokerAddress  string
	rabbitmqServer string
	disableSend    bool
}

func (pc *productionCommand) runCommand(cmd *cobra.Command, args []string) error {
	data := common.OperationProductionData{
		ProductionInfo: common.ProductionInfo{
			System: pc.system,
			Type:   common.ProductionType(pc.productionType),
		},
		StartTime:    pc.startTime,
		ForecastTime: pc.forecastTime,
		ProductionEventStatus: common.ProductionEventStatus{
			Event:  common.ProductionEvent(pc.event),
			Status: common.ToEventStatus(pc.status),
		},
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
	routeKeyName := fmt.Sprintf("%s.production.%s", pc.system, pc.productionType)

	if pc.disableSend {
		log.WithFields(log.Fields{
			"component": "production",
			"event":     "send",
		}).Infof("message deliver is disabled by --disable-send option.")
		return nil
	}

	senderType := RabbitMQSenderType
	if pc.useBroker {
		senderType = BrokerSenderType
	}

	return sendMessage(senderType, rabbitmqServer, exchangeName, routeKeyName, messageBytes)
}

func newProductionCommand() *productionCommand {
	pc := &productionCommand{}

	productionCmd := &cobra.Command{
		Use:   "production",
		Short: "send production messages",
		Long:  productionDescription,
		RunE:  pc.runCommand,
	}

	productionCmd.Flags().StringVar(&pc.system, "system", "",
		"system name, such as grapes_gfs_gmf")
	productionCmd.Flags().StringVar(&pc.productionType, "production-type", "",
		fmt.Sprintf("production type, such as %s", common.ProductionTypeGrib2))
	productionCmd.Flags().StringVar(&pc.event, "event", "",
		fmt.Sprintf("production event, such as %s", common.ProductionEventStorage))
	productionCmd.Flags().StringVar(&pc.status, "status", string(common.Complete),
		fmt.Sprintf("event status, such as %s, %s", common.Complete, common.Aborted))

	productionCmd.Flags().StringVar(&pc.startTime, "start-time", "",
		"start time, YYYYMMDDHH")
	productionCmd.Flags().StringVar(&pc.forecastTime, "forecast-time", "",
		"forecast time, FFFh, 0h, 12h, ...")

	productionCmd.Flags().StringVar(&pc.rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")

	productionCmd.Flags().BoolVar(&pc.useBroker, "with-broker", true,
		"deliver message using a broker, should set --broker-address when enabled.")
	productionCmd.Flags().StringVar(&pc.brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")

	productionCmd.Flags().BoolVar(&pc.disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

	productionCmd.MarkFlagRequired("system")
	productionCmd.MarkFlagRequired("production-type")
	productionCmd.MarkFlagRequired("event")
	productionCmd.MarkFlagRequired("start-time")
	productionCmd.MarkFlagRequired("forecast-time")

	productionCmd.MarkFlagRequired("rabbitmq-server")

	pc.cmd = productionCmd
	return pc
}
