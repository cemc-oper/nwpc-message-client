package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/streadway/amqp"
	"google.golang.org/grpc"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"
)

const brokerDescription = `
A broker for nwpc_message_client command. 
Messages will be transmitted to a rabbitmq server without any changes.

Tasks running on parallel nodes should connect a broker running on a login node to send messages.
`

type brokerCommand struct {
	BaseCommand

	brokerAddress  string
	disableDeliver bool

	brokerMode string

	enableProfiling  bool
	profilingAddress string
}

func (bc *brokerCommand) runCommand(cmd *cobra.Command, args []string) error {

	log.WithFields(log.Fields{
		"component": "broker",
		"event":     "connection",
	}).Infof("listening on %s", bc.brokerAddress)
	lis, err := net.Listen("tcp", bc.brokerAddress)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "broker",
			"event":     "connection",
		}).Errorf("failed to listen: %v", err)
		return fmt.Errorf("failed to listen: %v", err)
	}

	if bc.enableProfiling {
		log.Infof("enable profiling...%s", bc.profilingAddress)
		go func() {
			log.Println(http.ListenAndServe(bc.profilingAddress, nil))
		}()
	}

	grpcServer := grpc.NewServer()

	server := &common.MessageBrokerServer{
		DisableDeliver: bc.disableDeliver,
		BrokerMode:     bc.brokerMode,
	}

	if bc.brokerMode == "batch" {
		messageChan := make(chan common.RabbitMQMessage, 10)
		server.MessageChan = messageChan

		go publishToRabbitMQ(messageChan)
	}

	pb.RegisterMessageBrokerServer(grpcServer, server)
	err = grpcServer.Serve(lis)
	return err
}

func newBrokerCommand() *brokerCommand {
	bc := &brokerCommand{}

	brokerCmd := &cobra.Command{
		Use:   "broker",
		Short: "A broker for NWPC Message Client",
		Long:  brokerDescription,
		RunE:  bc.runCommand,
	}

	brokerCmd.Flags().StringVar(
		&bc.brokerAddress,
		"address",
		":33383",
		"broker rpc address, use tcp port.",
	)

	brokerCmd.Flags().StringVar(
		&bc.brokerMode,
		"mode",
		"direct",
		"broker mode, direct or batch",
	)

	brokerCmd.Flags().BoolVar(
		&bc.disableDeliver,
		"disable-deliver",
		false,
		"disable deliver messages to message queue, just for debug.",
	)

	brokerCmd.Flags().BoolVar(
		&bc.enableProfiling,
		"enable-profiling",
		false,
		"enable profiling, just for debug.",
	)
	brokerCmd.Flags().StringVar(
		&bc.profilingAddress,
		"profiling-address",
		"127.0.0.1:31485",
		"profiling address, just for debug.",
	)

	bc.cmd = brokerCmd
	return bc
}

const BulkSize = 100

func publishToRabbitMQ(messageChan chan common.RabbitMQMessage) {
	var received []common.RabbitMQMessage
	for {
		select {
		case message := <-messageChan:
			received = append(received, message)
			if len(received) > BulkSize {
				//log.WithFields(log.Fields{
				//	"component": "broker",
				//	"event":     "batch-publish",
				//}).Infof("begin to publish")
				sendBatchMessages(received)
				received = nil
			}
		case <-time.After(time.Second * 2):
			//log.WithFields(log.Fields{
			//	"component": "broker",
			//	"event":     "batch-publish",
			//}).Infof("time check: %d", len(received))
			if len(received) > 0 {
				log.WithFields(log.Fields{
					"component": "broker",
					"event":     "batch-publish",
				}).Infof("begin to publish")
				sendBatchMessages(received)
				received = nil
			}
		}
	}
}

func sendBatchMessages(messages []common.RabbitMQMessage) {
	messageByServer := make(map[string][]common.RabbitMQMessage)
	for _, message := range messages {
		target := message.Target
		messageByServer[target.Server] = append(messageByServer[target.Server], message)
	}

	for server, messagesInServer := range messageByServer {
		//log.WithFields(log.Fields{
		//	"component": "broker",
		//	"event":     "batch-send",
		//}).Infof("find server: %s . %d", server, len(messagesInServer))
		connection, err := amqp.Dial(server)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "broker",
				"event":     "batch-send",
			}).Errorf("failed to create connection: %v", err)
			continue
		}
		defer connection.Close()

		channel, err := connection.Channel()
		if err != nil {
			log.WithFields(log.Fields{
				"component": "broker",
				"event":     "batch-send",
			}).Errorf("failed to create channel: %v", err)
			continue
		}
		defer channel.Close()

		for _, message := range messagesInServer {
			err = channel.Publish(
				message.Target.Exchange,
				message.Target.RouteKey,
				false,
				false,
				amqp.Publishing{
					ContentType:  "text/plain",
					DeliveryMode: amqp.Persistent,
					Body:         message.Message,
				})
			if err != nil {
				log.WithFields(log.Fields{
					"component": "broker",
					"event":     "batch-send",
				}).Errorf("failed to send message: %v", err)
			}
		}
	}

}

func sendToRabbitMQ(message common.RabbitMQMessage, channel *amqp.Channel) error {
	err := channel.Publish(
		message.Target.Exchange,
		message.Target.RouteKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         message.Message,
		})
	if err != nil {
		return fmt.Errorf("publish message has error: %s", err)
	}

	return nil
}
