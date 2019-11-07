package app

import (
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/messagebroker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"net"
)

func init() {
	rootCmd.AddCommand(brokerCmd)

	brokerCmd.Flags().StringVar(&brokerAddress, "address", ":33383", "broker rpc address")
}

const brokerDescription = `
A broker for nwpc_message_client ecflow-client command.
Tasks running on parallel nodes should connect a broker on a login node to send messages.
`

var brokerCmd = &cobra.Command{
	Use:   "broker",
	Short: "broker for nwpc_message_client",
	Long:  brokerDescription,
	Run: func(cmd *cobra.Command, args []string) {
		log.WithFields(log.Fields{
			"component": "broker",
			"event":     "server",
		}).Infof("listening on %s", brokerAddress)
		lis, err := net.Listen("tcp", brokerAddress)
		if err != nil {
			log.WithFields(log.Fields{
				"component": "broker",
				"event":     "server",
			}).Fatalf("failed to listen: %v", err)
		}

		grpcServer := grpc.NewServer()

		server := &common.MessageBrokerServer{}

		pb.RegisterMessageBrokerServer(grpcServer, server)
		grpcServer.Serve(lis)
	},
}
