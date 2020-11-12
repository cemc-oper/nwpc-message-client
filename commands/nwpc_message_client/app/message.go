package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/commands"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"os"
	"time"
)

const messageDescription = `
Send messages from json file. 
Messages are send to a rabbitmq server directly or via a broker running by nwpc_message_client broker command.
`

func newMessageCommand() *messageCommand {
	mc := &messageCommand{
		targetParser: targetParser{
			defaultOption: targetOptions{
				writeTimeout: 2 * time.Second,
				useBroker:    true,
				brokerTries:  2,
				exchangeName: "nwpc.operation.workflow",
				routeKeyName: "ecflow.command.ecflow_client",
			},
		},
	}
	messageCmd := &cobra.Command{
		Use:                "message",
		Short:              "send messages",
		Long:               messageDescription,
		RunE:               mc.runCommand,
		DisableFlagParsing: true,
	}
	messageCmd.SetUsageFunc(func(*cobra.Command) error {
		mc.printHelp()
		return nil
	})
	messageCmd.SetHelpFunc(func(*cobra.Command, []string) {
		mc.printHelp()
	})

	mc.cmd = messageCmd
	return mc
}

type messageCommand struct {
	BaseCommand

	mainOptions struct {
		messageBody  string
		exchangeName string
		routeKeyName string
		help         bool
	}

	targetParser
}

func (mc *messageCommand) runCommand(cmd *cobra.Command, args []string) error {
	err := mc.parseMainOptions(args)
	if err != nil {
		return fmt.Errorf("parse main options has eror: %v", err)
	}
	if mc.mainOptions.help {
		mc.printHelp()
		return nil
	}

	err = mc.targetParser.parseCommandTargetOptions(args)
	if err != nil {
		return fmt.Errorf("parse target options has eror: %v", err)
	}

	messageBytes := []byte(mc.mainOptions.messageBody)

	return sendMessageBytesToTarget(mc.targetParser.option, messageBytes)
}

func (mc *messageCommand) generateMainFlags() *pflag.FlagSet {
	mainFlagSet := pflag.NewFlagSet("main", pflag.ContinueOnError)
	mainFlagSet.ParseErrorsWhitelist.UnknownFlags = true

	mainFlagSet.StringVar(&mc.mainOptions.messageBody, "message-body", "",
		"message body, json bytes.")
	mainFlagSet.BoolVar(&mc.mainOptions.help, "help", false, "print usage")

	mainFlagSet.SetAnnotation("message-body", commands.RequiredOption, []string{"true"})
	mainFlagSet.SetAnnotation("exchange-name", commands.RequiredOption, []string{"true"})
	mainFlagSet.SetAnnotation("route-key-name", commands.RequiredOption, []string{"true"})

	return mainFlagSet
}

func (mc *messageCommand) parseMainOptions(args []string) error {
	mainFlagSet := mc.generateMainFlags()
	err := mainFlagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}
	if mc.mainOptions.help {
		return nil
	}

	err = commands.CheckRequiredFlags(mainFlagSet)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	return nil
}

func (mc *messageCommand) printHelp() {
	helpOutput := os.Stdout
	fmt.Fprintf(helpOutput, "%s\n", ecflowClientDescription)

	mainFlags := mc.generateMainFlags()
	mainFlags.SetOutput(helpOutput)
	fmt.Fprintf(helpOutput, "Main Flags:\n")
	mainFlags.PrintDefaults()

	fmt.Fprintf(helpOutput, "\n")
	targetFlags := mc.targetParser.generateFlags()
	targetFlags.SetOutput(helpOutput)
	fmt.Fprintf(helpOutput, "Target Flags:\n")
	targetFlags.PrintDefaults()
}
