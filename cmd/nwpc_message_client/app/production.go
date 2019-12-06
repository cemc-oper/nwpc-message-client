package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	"github.com/nwpc-oper/nwpc-message-client/common/sender"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"time"
)

func init() {
	rootCmd.AddCommand(productionCmd)

	productionCmd.Flags().StringVar(&system, "system", "", "system")
	productionCmd.Flags().StringVar(&productionType, "production-type", "", "production type")
	productionCmd.Flags().StringVar(&event, "event", "", "event, such as storage")
	productionCmd.Flags().StringVar(&status, "status", "completed", "status")
	productionCmd.Flags().StringVar(&startTime, "start-time", "", "start time, YYYYMMDDHH")
	productionCmd.Flags().StringVar(&forecastTime, "forecast-time", "", "forecast time, FFFh, 0h, 12h, ...")

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

const productionMessageType = "production"
const productionDescription = `
Send messages for production.
Messages are send to a rabbitmq server via a broker running by broker command.
`

const (
	productionExchangeName = "nwpc.operation.production"
	productionWriteTimeOut = 2 * time.Second
)

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
			App:  "nwpc-message-client",
			Type: productionMessageType,
			Time: time.Now(),
			Data: data,
		}

		messageBytes, _ := json.Marshal(message)

		log.WithFields(log.Fields{
			"component": "production",
			"event":     "message",
		}).Infof("%s", messageBytes)

		if useBroker {
			return sendProductionMessageWithBroker(messageBytes)
		} else {
			return sendProductionMessage(messageBytes)
		}
	},
}

func sendProductionMessageWithBroker(messageBytes []byte) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(brokerAddress, opts...)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "production",
			"event":     "connection",
		}).Errorf("connect to broker has error: %v\n", err)
		return fmt.Errorf("connect to broker has error: %v\n", err)
	}

	defer conn.Close()

	client := pb.NewMessageBrokerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	response, err := client.SendRabbitMQMessage(ctx, &pb.RabbitMQMessage{
		Target: &pb.RabbitMQTarget{
			Server:   rabbitmqServer,
			Exchange: productionExchangeName,
			RouteKey: fmt.Sprintf("%s.production.%s", system, productionType),
		},
		Message: &pb.Message{
			Data: messageBytes,
		},
	})

	if err != nil {
		log.WithFields(log.Fields{
			"component": "production",
			"event":     "send",
		}).Errorf("send message has error: %v", err)
		return fmt.Errorf("send message has error: %v", err)
	}

	if response.ErrorNo != 0 {
		log.WithFields(log.Fields{
			"component": "production",
			"event":     "response",
		}).Errorf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
		return fmt.Errorf("send message return error code %d: %s", response.ErrorNo, response.ErrorMessage)
	}
	return nil
}

func sendProductionMessage(messageBytes []byte) error {
	rabbitmqTarget := sender.RabbitMQTarget{
		Server:       rabbitmqServer,
		Exchange:     productionExchangeName,
		RouteKey:     fmt.Sprintf("%s.production.%s", system, productionType),
		WriteTimeout: productionWriteTimeOut,
	}

	rabbitSender := sender.RabbitMQSender{
		Target: rabbitmqTarget,
		Debug:  true,
	}

	if !disableDeliver {
		err := rabbitSender.SendMessage(messageBytes)
		if err != nil {
			return fmt.Errorf("send messge has error: %s", err)
		}
	}

	return nil
}
