package app

import (
	"fmt"
	"github.com/nwpc-oper/nwpc-message-client/commands"
	"github.com/spf13/pflag"
	"time"
)

type targetOptions struct {
	rabbitmqServer string
	writeTimeout   time.Duration

	useBroker     bool
	brokerAddress string

	disableSend bool
}

func (t *targetOptions) parseCommandTargetOptions(args []string) error {
	targetFlagSet := pflag.NewFlagSet("targetFlagSet", pflag.ContinueOnError)
	targetFlagSet.ParseErrorsWhitelist.UnknownFlags = true

	targetFlagSet.StringVar(&t.rabbitmqServer, "rabbitmq-server", "",
		"rabbitmq server, such as amqp://guest:guest@host:port")

	targetFlagSet.BoolVar(&t.useBroker, "with-broker", false,
		"deliver message using a broker, should set --broker-address when enabled.")
	targetFlagSet.StringVar(&t.brokerAddress, "broker-address", "",
		"broker address, work with --with-broker")

	targetFlagSet.BoolVar(&t.disableSend, "disable-send", false,
		"disable message deliver, just for debug.")

	targetFlagSet.SetAnnotation("rabbitmq-server", commands.RequiredOption, []string{"true"})

	err := targetFlagSet.Parse(args)
	if err != nil {
		return fmt.Errorf("parse options has error: %s", err)
	}

	err = commands.CheckRequiredFlags(targetFlagSet)
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	return nil
}
