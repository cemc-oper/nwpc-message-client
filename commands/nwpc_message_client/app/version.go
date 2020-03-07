package app

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	Version   = "Unknown version"
	BuildTime = "Unknown build time"
	GitCommit = "Unknown GitCommit"
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

	cmd := &cobra.Command{
		Use:   "version",
		Short: "print version information",
		Long:  "Print version information",
		Run:   vc.showVersion,
	}

	vc.cmd = cmd
	return vc
}
