package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"net"
	"net/http"
	_ "net/http/pprof"
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
