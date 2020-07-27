package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/commands"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/spf13/pflag"
	"time"
)

type OperationPropertiesGenerator struct {
	common.OperationProductionProperties
	options struct {
		startTime    string
		forecastTime string
	}
}

func (parser *OperationPropertiesGenerator) generateFlags() *pflag.FlagSet {
	operFlagSet := pflag.NewFlagSet("operation", pflag.ContinueOnError)
	operFlagSet.SortFlags = false
	operFlagSet.ParseErrorsWhitelist.UnknownFlags = true

	operFlagSet.StringVar(&parser.options.startTime, "start-time", "",
		"start time, YYYYMMDDHH")
	operFlagSet.StringVar(&parser.options.forecastTime, "forecast-time", "",
		"forecast time, FFFh, 0h, 12h, ...")
	operFlagSet.SetAnnotation("start-time", commands.RequiredOption, []string{"true"})
	operFlagSet.SetAnnotation("forecast-time", commands.RequiredOption, []string{"true"})
	return operFlagSet
}

func (parser *OperationPropertiesGenerator) parseOptions(args []string) error {
	operFlagSet := parser.generateFlags()
	err := operFlagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}

	err = commands.CheckRequiredFlags(operFlagSet)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	startTime, err := time.Parse("2006010215", parser.options.startTime)
	if err != nil {
		return fmt.Errorf("parse start time %s has error: %v", parser.options.startTime, err)
	}
	parser.OperationProductionProperties = common.OperationProductionProperties{
		StartTime:    startTime,
		ForecastTime: parser.options.forecastTime,
	}

	return nil
}
