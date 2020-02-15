package app

import (
	"fmt"
	"github.com/spf13/cobra"
)

type versionCommand struct {
	cmd *cobra.Command
}

func (c *versionCommand) getCommand() *cobra.Command {
	return c.cmd
}

func newVersionCommand() *versionCommand {
	return &versionCommand{
		cmd: &cobra.Command{
			Use:   "version",
			Short: "Print version information",
			Long:  "Print version information",
			Run: func(cmd *cobra.Command, args []string) {
				fmt.Printf("Version %s (%s)\n", Version, GitCommit)
				fmt.Printf("Build at %s\n", BuildTime)
			},
		},
	}
}
