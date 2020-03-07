package app

import (
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/commands"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"strconv"
	"time"
)

const ProductionMessageType = "production"

const productionDescription = `
Send messages for production.
Messages are send to a rabbitmq server directly or via a broker running by broker command.
`

type productionCommand struct {
	BaseCommand

	common.ProductionInfo
	common.ProductionEventStatus

	mainOptions struct {
		system         string
		stream         string
		productionType string
		product        string

		event  string
		status string
	}

	rabbitmqServer string
	writeTimeout   time.Duration

	useBroker     bool
	brokerAddress string

	disableSend bool
}

func (pc *productionCommand) runCommand(cmd *cobra.Command, args []string) error {
	err := pc.parseMainOptions(args)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	data, err := pc.createProductionData(args)

	if err != nil {
		return fmt.Errorf("create production data has error: %s", err)
	}

	return pc.sendProductionMessage(data)
}

func newProductionCommand() *productionCommand {
	pc := &productionCommand{
		writeTimeout: 2 * time.Second,
	}

	productionCmd := &cobra.Command{
		Use:                "production",
		Short:              "send production messages",
		Long:               productionDescription,
		RunE:               pc.runCommand,
		DisableFlagParsing: true,
	}

	pc.cmd = productionCmd
	return pc
}

func (pc *productionCommand) parseMainOptions(args []string) error {
	flagSet := pc.generateCommandMainParser()
	err := flagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}

	err = commands.CheckRequiredFlags(flagSet)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	pc.fillProductionInfo()
	pc.fillProductionEventStatus()

	return nil
}

func (pc *productionCommand) generateCommandMainParser() *pflag.FlagSet {
	flagSet := pflag.NewFlagSet("oper", pflag.ContinueOnError)
	flagSet.ParseErrorsWhitelist.UnknownFlags = true
	flagSet.StringVar(&pc.mainOptions.system, "system", "",
		"system name, such as grapes_gfs_gmf")
	flagSet.StringVar((*string)(&pc.mainOptions.productionType), "production-type", "",
		fmt.Sprintf("production type, such as %s", common.ProductionTypeGrib2))
	flagSet.StringVar((*string)(&pc.mainOptions.stream), "production-stream", "",
		"production stream, such as oper")
	flagSet.StringVar(&pc.mainOptions.product, "production-name", "",
		"production name, such as orig")

	flagSet.StringVar(&pc.mainOptions.event, "event", "",
		fmt.Sprintf("production event, such as %s", common.ProductionEventStorage))
	flagSet.StringVar(&pc.mainOptions.status, "status", string(common.Complete),
		fmt.Sprintf("event status, such as %s, %s", common.Complete, common.Aborted))

	flagSet.StringVar(&pc.rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")

	flagSet.BoolVar(&pc.useBroker, "with-broker", true,
		"deliver message using a broker, should set --broker-address when enabled.")
	flagSet.StringVar(&pc.brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")

	flagSet.BoolVar(&pc.disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

	flagSet.SortFlags = false
	flagSet.ParseErrorsWhitelist = pflag.ParseErrorsWhitelist{UnknownFlags: true}

	flagSet.SetAnnotation("system", commands.RequiredOption, []string{"true"})
	flagSet.SetAnnotation("production-type", commands.RequiredOption, []string{"true"})
	flagSet.SetAnnotation("production-stream", commands.RequiredOption, []string{"true"})
	flagSet.SetAnnotation("production-name", commands.RequiredOption, []string{"true"})
	flagSet.SetAnnotation("event", commands.RequiredOption, []string{"true"})
	flagSet.SetAnnotation("rabbitmq-server", commands.RequiredOption, []string{"true"})
	return flagSet
}

func (pc *productionCommand) fillProductionInfo() {
	pc.ProductionInfo = common.ProductionInfo{
		System:  pc.mainOptions.system,
		Stream:  common.ProductionStream(pc.mainOptions.stream),
		Type:    common.ProductionType(pc.mainOptions.productionType),
		Product: common.ProductionName(pc.mainOptions.product),
	}
}

func (pc *productionCommand) fillProductionEventStatus() {
	status, _ := strconv.Atoi(pc.mainOptions.status)
	pc.ProductionEventStatus = common.ProductionEventStatus{
		Event:  common.ProductionEvent(pc.mainOptions.event),
		Status: common.EventStatus(status),
	}
}

func (pc *productionCommand) createProductionData(args []string) (interface{}, error) {
	var data interface{}
	var err error
	switch pc.ProductionInfo.Stream {
	case common.ProductionStreamOperation:
		data, err = pc.getOperationData(args)
		break
	case common.ProductionStreamEPS:
	default:
		err = fmt.Errorf("stream type is not supported: %s", pc.ProductionInfo.Stream)
	}

	return data, err
}

func (pc *productionCommand) getOperationData(
	args []string,
) (common.OperationProductionData, error) {
	generator := OperationPropertiesGenerator{}
	err := generator.parseOptions(args)
	if err != nil {
		return common.OperationProductionData{}, err
	}

	data := common.OperationProductionData{
		ProductionInfo:                pc.ProductionInfo,
		OperationProductionProperties: generator.OperationProductionProperties,
		ProductionEventStatus:         pc.ProductionEventStatus,
	}
	return data, nil
}

type OperationPropertiesGenerator struct {
	common.OperationProductionProperties
	options struct {
		startTime    string
		forecastTime string
	}
}

func (parser *OperationPropertiesGenerator) parseOptions(args []string) error {
	operFlagSet := pflag.NewFlagSet("oper", pflag.ContinueOnError)
	operFlagSet.ParseErrorsWhitelist.UnknownFlags = true

	operFlagSet.StringVar(&parser.options.startTime, "start-time", "",
		"start time, YYYYMMDDHH")
	operFlagSet.StringVar(&parser.options.forecastTime, "forecast-time", "",
		"forecast time, FFFh, 0h, 12h, ...")
	operFlagSet.SetAnnotation("start-time", commands.RequiredOption, []string{"true"})
	operFlagSet.SetAnnotation("forecast-time", commands.RequiredOption, []string{"true"})

	err := operFlagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}

	err = commands.CheckRequiredFlags(operFlagSet)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	parser.OperationProductionProperties = common.OperationProductionProperties{
		StartTime:    parser.options.startTime,
		ForecastTime: parser.options.forecastTime,
	}

	return nil
}

func (pc *productionCommand) sendProductionMessage(data interface{}) error {
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
	routeKeyName := fmt.Sprintf("%s.production.%s", pc.ProductionInfo.System, pc.ProductionInfo.Type)

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
