package app

import (
	"github.com/nwpc-oper/nwpc-message-client/common"
	pb "github.com/nwpc-oper/nwpc-message-client/messagebroker"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"log"
	"net"
)

func init() {
	rootCmd.AddCommand(brokerCmd)

	brokerCmd.Flags().StringVar(&brokerAddress, "address", ":33383", "broker rpc address")
}

var brokerCmd = &cobra.Command{
	Use:   "broker",
	Short: "broker for nwpc_message_client",
	Long:  "broker for nwpc_message_client",
	Run: func(cmd *cobra.Command, args []string) {
		log.Printf("listening on %s", brokerAddress)
		lis, err := net.Listen("tcp", brokerAddress)
		if err != nil {
			log.Fatalf("failed to listem: %v", err)
		}

		grpcServer := grpc.NewServer()

		server := &common.MessageBrokerServer{}

		pb.RegisterMessageBrokerServer(grpcServer, server)
		grpcServer.Serve(lis)
	},
}
