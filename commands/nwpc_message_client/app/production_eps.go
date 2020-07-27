package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/commands"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/spf13/pflag"
	"time"
)

type EpsPropertiesGenerator struct {
	common.EpsProductionProperties
	options struct {
		startTime    string
		forecastTime string
		number       int
	}
}

func (parser *EpsPropertiesGenerator) generateFlags() *pflag.FlagSet {
	epsFlagSet := pflag.NewFlagSet("eps", pflag.ContinueOnError)
	epsFlagSet.SortFlags = false
	epsFlagSet.ParseErrorsWhitelist.UnknownFlags = true

	epsFlagSet.StringVar(&parser.options.startTime, "start-time", "",
		"start time, YYYYMMDDHH")
	epsFlagSet.StringVar(&parser.options.forecastTime, "forecast-time", "",
		"forecast time, FFFh, 0h, 12h, ...")
	epsFlagSet.IntVar(&parser.options.number, "number", 0,
		"member number, such as 0, 1, 2, ...")
	epsFlagSet.SetAnnotation("start-time", commands.RequiredOption, []string{"true"})
	epsFlagSet.SetAnnotation("forecast-time", commands.RequiredOption, []string{"true"})
	return epsFlagSet
}

func (parser *EpsPropertiesGenerator) parseOptions(args []string) error {
	epsFlagSet := parser.generateFlags()
	err := epsFlagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}

	err = commands.CheckRequiredFlags(epsFlagSet)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	startTime, err := time.Parse("2006010215", parser.options.startTime)
	if err != nil {
		return fmt.Errorf("parse start time %s has error: %v", parser.options.startTime, err)
	}
	parser.EpsProductionProperties = common.EpsProductionProperties{
		StartTime:    startTime,
		ForecastTime: parser.options.forecastTime,
		Number:       parser.options.number,
	}

	return nil
}
