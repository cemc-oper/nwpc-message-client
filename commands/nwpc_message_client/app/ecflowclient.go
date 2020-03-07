package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/commands"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"time"
)

const EcflowClientMessageType = "ecflow-client"
const ecflowClientDescription = `
Send messages for ecflow_client command. 
Messages are send to a rabbitmq server directly or via a broker running by nwpc_message_client broker command.
`

type ecflowClientCommand struct {
	BaseCommand

	mainOptions struct {
		commandOptions string
	}

	targetOptions
}

func (ec *ecflowClientCommand) runCommand(cmd *cobra.Command, args []string) error {
	err := ec.parseCommandMainOptions(args)
	if err != nil {
		return err
	}

	err = ec.targetOptions.parseCommandTargetOptions(args)
	if err != nil {
		return err
	}

	data, err := common.CreateEcflowClientMessage(ec.mainOptions.commandOptions)
	if err != nil {
		return err
	}

	message := common.EventMessage{
		App:  appName,
		Type: EcflowClientMessageType,
		Time: time.Now(),
		Data: data,
	}

	return sendToTarget(ec.targetOptions, message)
}

func (ec *ecflowClientCommand) parseCommandMainOptions(args []string) error {
	mainFlagSet := pflag.NewFlagSet("mainFlagSet", pflag.ContinueOnError)
	mainFlagSet.ParseErrorsWhitelist.UnknownFlags = true
	mainFlagSet.StringVar(&ec.mainOptions.commandOptions, "command-options", "",
		"ecflow_client command options")

	mainFlagSet.SetAnnotation("command-options", commands.RequiredOption, []string{"true"})

	err := mainFlagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}

	err = commands.CheckRequiredFlags(mainFlagSet)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	return nil
}

func newEcflowClientCommand() *ecflowClientCommand {
	ec := &ecflowClientCommand{
		targetOptions: targetOptions{
			writeTimeout: 2 * time.Second,
			useBroker:    true,
			exchangeName: "nwpc.operation.workflow",
			routeKeyName: "ecflow.command.ecflow_client",
		},
	}
	ecFlowClientCmd := &cobra.Command{
		Use:                "ecflow-client",
		Short:              "send messages for ecflow_client command",
		Long:               ecflowClientDescription,
		RunE:               ec.runCommand,
		DisableFlagParsing: true,
	}

	ec.cmd = ecFlowClientCmd
	return ec
}
