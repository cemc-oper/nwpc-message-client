package app

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"strings"
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
	stream         string
	productionType string

	event  string
	status string

	rabbitmqServer string
	writeTimeout   time.Duration

	useBroker     bool
	brokerAddress string

	disableSend bool
}

func (pc *productionCommand) runCommand(cmd *cobra.Command, args []string) error {
	var data interface{}
	var err error
	switch common.ProductionStream(pc.stream) {
	case common.ProductionStreamOperation:
		data, err = pc.getOperationData(cmd, args)
		break
	case common.ProductionStreamEPS:
	default:
		err = fmt.Errorf("stream type is not supported: %s", pc.stream)
	}

	if err != nil {
		return fmt.Errorf("create production data has error: %s", err)
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

	var currentSender sender.Sender
	switch senderType {
	case RabbitMQSenderType:
		currentSender = sender.CreateRabbitMQSender(
			pc.rabbitmqServer, exchangeName, routeKeyName, pc.writeTimeout)
		break
	case BrokerSenderType:
		currentSender = sender.CreateBrokerSender(
			pc.brokerAddress, pc.rabbitmqServer, exchangeName, routeKeyName, pc.writeTimeout)
		break
	default:
		return fmt.Errorf("SenderType is not supported: %d", senderType)
	}

	return sendMessage(currentSender, messageBytes)
}

func newProductionCommand() *productionCommand {
	pc := &productionCommand{
		writeTimeout: 2 * time.Second,
	}

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
	productionCmd.Flags().StringVar(&pc.stream, "production-stream", "",
		"production stream, such as oper")

	productionCmd.Flags().StringVar(&pc.event, "event", "",
		fmt.Sprintf("production event, such as %s", common.ProductionEventStorage))
	productionCmd.Flags().StringVar(&pc.status, "status", string(common.Complete),
		fmt.Sprintf("event status, such as %s, %s", common.Complete, common.Aborted))

	productionCmd.Flags().StringVar(&pc.rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")

	productionCmd.Flags().BoolVar(&pc.useBroker, "with-broker", true,
		"deliver message using a broker, should set --broker-address when enabled.")
	productionCmd.Flags().StringVar(&pc.brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")

	productionCmd.Flags().BoolVar(&pc.disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

	productionCmd.Flags().SortFlags = false

	productionCmd.MarkFlagRequired("system")
	productionCmd.MarkFlagRequired("production-type")
	productionCmd.MarkFlagRequired("production-stream")
	productionCmd.MarkFlagRequired("event")

	productionCmd.MarkFlagRequired("rabbitmq-server")

	pc.cmd = productionCmd
	return pc
}

const RequiredOption = "REQUIRED_OPTION"

func (pc *productionCommand) getOperationData(
	cmd *cobra.Command, args []string,
) (common.OperationProductionData, error) {
	var startTime string
	var forecastTime string

	operFlagSet := pflag.NewFlagSet("oper", pflag.ContinueOnError)
	operFlagSet.ParseErrorsWhitelist.UnknownFlags = true

	operFlagSet.StringVar(&startTime, "start-time", "",
		"start time, YYYYMMDDHH")
	operFlagSet.StringVar(&forecastTime, "forecast-time", "",
		"forecast time, FFFh, 0h, 12h, ...")
	operFlagSet.SetAnnotation("start-time", RequiredOption, []string{"true"})
	operFlagSet.SetAnnotation("forecast-time", RequiredOption, []string{"true"})

	err := operFlagSet.Parse(args)
	if err != nil {
		return common.OperationProductionData{}, fmt.Errorf("parse options has error: %s", err)
	}

	var missingFlagNames []string
	operFlagSet.VisitAll(func(flag *pflag.Flag) {
		requiredAnnotation, found := flag.Annotations[RequiredOption]
		if !found {
			return
		}
		if (requiredAnnotation[0] == "true") && !flag.Changed {
			missingFlagNames = append(missingFlagNames, flag.Name)
		}
	})
	if len(missingFlagNames) > 0 {
		return common.OperationProductionData{},
			fmt.Errorf(`required flag(s) "%s" not set`, strings.Join(missingFlagNames, `", "`))
	}

	data := common.OperationProductionData{
		ProductionInfo: common.ProductionInfo{
			System: pc.system,
			Type:   common.ProductionType(pc.productionType),
		},
		StartTime:    startTime,
		ForecastTime: forecastTime,
		ProductionEventStatus: common.ProductionEventStatus{
			Event:  common.ProductionEvent(pc.event),
			Status: common.ToEventStatus(pc.status),
		},
	}
	return data, nil
}
