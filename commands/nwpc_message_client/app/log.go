package app

import (
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/jessevdk/go-flags"
	"github.com/nwpc-oper/nwpc-message-client/commands"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"os"
	"time"
)

const logDescription = `
Send log message for operation systems.
Messages are send to a rabbitmq server directly or via a broker running by broker command.
`

func newLogCommand() *logCommand {
	lc := &logCommand{
		targetParser: targetParser{
			defaultOption: targetOptions{
				brokerTries:  2,
				writeTimeout: 2 * time.Second,
				exchangeName: "nwpc.operation.log",
			},
		},
	}

	logCmd := &cobra.Command{
		Use:                "log",
		Short:              "send log message",
		Long:               logDescription,
		RunE:               lc.runCommand,
		DisableFlagParsing: true,
	}

	lc.cmd = logCmd
	return lc
}

type logCommand struct {
	BaseCommand

	mainOptions struct {
		system    string
		startTime string
		time      string
		level     string
		logType   string
		help      bool
	}

	contentOptions struct {
		content map[string]string
	}

	targetParser
}

// run the log command: parse all options, generate message, and send it.
func (lc *logCommand) runCommand(cmd *cobra.Command, args []string) error {
	// parse main
	err := lc.parseMainOptions(args)
	if lc.mainOptions.help {
		lc.printHelp()
		return nil
	}
	if err != nil {
		return fmt.Errorf("parse main options has error: %v", err)
	}

	// parse target
	err = lc.targetParser.parseCommandTargetOptions(args)
	if err != nil {
		return fmt.Errorf("parser target options has error: %v", err)
	}

	// parse content
	err = lc.parseContentOptions(args)
	if err != nil {
		return fmt.Errorf("parser content options has error: %v", err)
	}

	// generate data
	data, err := lc.createLogData()
	if err != nil {
		return fmt.Errorf("create log data has error: %v", err)
	}

	return lc.sendMessage(data)
}

// Parse main options
func (lc *logCommand) parseMainOptions(args []string) error {
	flagSet := lc.generateMainCommandParser()
	err := flagSet.Parse(args)

	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}

	if lc.mainOptions.help {
		return nil
	}

	err = commands.CheckRequiredFlags(flagSet)
	if err != nil {
		return fmt.Errorf("check required flags has error: %v", err)
	}

	return nil
}

// generate command parser for main options.
func (lc *logCommand) generateMainCommandParser() *pflag.FlagSet {
	flagSet := pflag.NewFlagSet("main", pflag.ContinueOnError)
	flagSet.ParseErrorsWhitelist.UnknownFlags = true
	flagSet.SortFlags = false

	flagSet.StringVar(&lc.mainOptions.system, "system", "", "system name, such as grapes_gfs_gmf")
	flagSet.StringVar(&lc.mainOptions.startTime, "start-time", "", "start time, YYYYMMDDHH")
	flagSet.StringVar(&lc.mainOptions.time, "time", "", "log time")
	flagSet.StringVar(&lc.mainOptions.level, "level", "", "log level")
	flagSet.StringVar(&lc.mainOptions.logType, "log-type", "", "log type")
	flagSet.BoolVar(&lc.mainOptions.help, "help", false, "print usage")

	flagSet.SetAnnotation("system", commands.RequiredOption, []string{"true"})
	flagSet.SetAnnotation("start-time", commands.RequiredOption, []string{"true"})
	flagSet.SetAnnotation("level", commands.RequiredOption, []string{"true"})
	flagSet.SetAnnotation("log-type", commands.RequiredOption, []string{"true"})

	return flagSet
}

// parse content options. Support arbitrary options.
func (lc *logCommand) parseContentOptions(args []string) error {
	// check content args
	flagSet := pflag.NewFlagSet("content", pflag.ContinueOnError)
	flagSet.ParseErrorsWhitelist.UnknownFlags = true
	flagSet.SortFlags = false

	err := flagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("check content options has error: %s", err)
	}

	dashIndex := flagSet.ArgsLenAtDash()
	if dashIndex == -1 {
		return nil
	}
	contentArgs := flagSet.Args()[dashIndex:]

	lc.contentOptions.content = make(map[string]string)

	// parse content options
	parser := flags.NewParser(nil, flags.Default)
	parser.UnknownOptionHandler = func(option string, arg flags.SplitArgument, args []string) (i []string, e error) {
		value, _ := arg.Value()
		if len(value) > 0 {
			lc.contentOptions.content[option] = value
		}
		return args, nil
	}
	_, err = parser.ParseArgs(contentArgs)
	if err != nil {
		return fmt.Errorf("parse content options has error: %v", err)
	}

	return nil
}

// create ``LogMessageData`` using options in ``logCommand``.
// Convert all strings into necessary data field.
func (lc *logCommand) createLogData() (interface{}, error) {
	startTime, err := time.Parse("2006010215", lc.mainOptions.startTime)
	if err != nil {
		return nil, fmt.Errorf("parse start time %s has error: %v", lc.mainOptions.startTime, err)
	}

	var logTime time.Time
	if len(lc.mainOptions.time) == 0 {
		logTime = time.Now()
	} else {
		logTime, err = dateparse.ParseAny(lc.mainOptions.time)
		if err != nil {
			return nil, fmt.Errorf("parse time %s has error: %v", lc.mainOptions.time, err)
		}
	}

	data := common.LogMessageData{
		System:    lc.mainOptions.system,
		StartTime: startTime,
		Time:      logTime,
		Level:     lc.mainOptions.level,
		Type:      lc.mainOptions.logType,
		Content:   lc.contentOptions.content,
	}

	return data, nil
}

// Send message to target.
// Create Event Message with message data.
func (lc *logCommand) sendMessage(data interface{}) error {
	message := common.EventMessage{
		App:  appName,
		Type: ProductionMessageType,
		Time: time.Now(),
		Data: data,
	}

	lc.targetParser.option.routeKeyName = fmt.Sprintf(
		"%s.log.%s", lc.mainOptions.system, lc.mainOptions.logType)

	return sendEventMessageToTarget(lc.targetParser.option, message)
}

// print command options usage.
func (lc *logCommand) printHelp() {
	helpOutput := os.Stdout
	fmt.Fprintf(helpOutput, "%s\n", logDescription)

	mainFlags := lc.generateMainCommandParser()
	mainFlags.SetOutput(helpOutput)
	fmt.Fprintf(helpOutput, "Main Flags:\n")
	mainFlags.PrintDefaults()

	fmt.Fprintf(helpOutput, `
Content Options:
	Put content fields after --. Support any option. For example:

	1. Background data
		-- --forecast-time=0h --source=ncep --start_hour_offset=-6
`)
}
