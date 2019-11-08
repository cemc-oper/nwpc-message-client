package app

import (
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "nwpc_message_consumer",
	Short: "A consumer for NWPC message.",
	Long:  "A consumer for NWPC message.",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
