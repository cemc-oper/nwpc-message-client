package app

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

func init() {
	fmt.Println("NWPC Message Client")
	fmt.Println("This program is under development. Please contact Wang Dapeng(3083) if it fails.")
	fmt.Println()
}

var rootCmd = &cobra.Command{
	Use:   "nwpc_message_clinet",
	Short: "A client for NWPC message.",
	Long:  "A client for NWPC message.",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
