package app

import (
	"fmt"
	"github.com/spf13/cobra"
)

type versionCommand struct {
	BaseCommand
}

func (vc *versionCommand) showVersion(cmd *cobra.Command, args []string) {
	fmt.Printf("Version %s (%s)\n", Version, GitCommit)
	fmt.Printf("Build at %s\n", BuildTime)
}

func newVersionCommand() *versionCommand {
	vc := &versionCommand{}
	vc.cmd = &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Long:  "Print version information",
		Run:   vc.showVersion,
	}
	return vc
}
