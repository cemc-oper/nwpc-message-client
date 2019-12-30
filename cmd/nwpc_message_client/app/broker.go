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

func init() {
	rootCmd.AddCommand(brokerCmd)

	brokerCmd.Flags().StringVar(&brokerAddress, "address", ":33383",
		"broker rpc address, use tcp port.")
	brokerCmd.Flags().BoolVar(&disableDeliver, "disable-deliver", false,
		"disable deliver messages to message queue, just for debug.")
	brokerCmd.Flags().BoolVar(&enableProfiling, "enable-profiling", false,
		"enable profiling, just for debug.")
	brokerCmd.Flags().StringVar(&profilingAddress, "profiling-address", "127.0.0.1:31485",
		"profiling address, just for debug.")
}

const brokerDescription = `
A broker for nwpc_message_client command. Messages will be transmitted to a rabbitmq server without any changes.

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

		if enableProfiling {
			log.Infof("enable profiling...%s", profilingAddress)
			go func() {
				log.Println(http.ListenAndServe(profilingAddress, nil))
			}()
		}

		grpcServer := grpc.NewServer()

		server := &common.MessageBrokerServer{
			DisableDeliver: disableDeliver,
		}

		pb.RegisterMessageBrokerServer(grpcServer, server)
		err = grpcServer.Serve(lis)
		return err
	},
}
