package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/common/messagebroker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"net"
)

func init() {
	rootCmd.AddCommand(brokerCmd)

	brokerCmd.Flags().StringVar(&brokerAddress, "address", ":33383", "broker rpc address")
	brokerCmd.Flags().BoolVar(&disableDeliver, "disable-deliver", false, "disable deliver messages to message queue.")
}

const brokerDescription = `
A broker for nwpc_message_client ecflow-client command.
Tasks running on parallel nodes should connect a broker on a login node to send messages.
`

var brokerCmd = &cobra.Command{
	Use:   "broker",
	Short: "broker for nwpc_message_client",
	Long:  brokerDescription,
	RunE: func(cmd *cobra.Command, args []string) error {
		log.WithFields(log.Fields{
			"component": "broker",
			"event":     "connection",
		}).Infof("listening on %s", brokerAddress)
		lis, err := net.Listen("tcp", brokerAddress)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "broker",
				"event":     "connection",
			}).Errorf("failed to listen: %v", err)
			return fmt.Errorf("failed to listen: %v", err)
		}

		grpcServer := grpc.NewServer()

		server := &common.MessageBrokerServer{
			DisableDeliver: disableDeliver,
		}

		pb.RegisterMessageBrokerServer(grpcServer, server)
		grpcServer.Serve(lis)
		return nil
	},
}
