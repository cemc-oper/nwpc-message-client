package app

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

func init() {
	fmt.Println("NWPC Message Consumer")
	fmt.Println("This program is under development. Please contact Wang Dapeng(3083) if it fails.")
	fmt.Println()
}

var rootCmd = &cobra.Command{
	Use:   "nwpc_message_consumer",
	Short: "A consumer for NWPC message.",
	Long:  "A consumer for NWPC message.",
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
