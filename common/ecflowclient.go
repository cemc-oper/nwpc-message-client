package common

import (
	"github.com/jessevdk/go-flags"
	"os"
	"strings"
)

func CreateEcflowClientMessage(commandOptions string) (*EcflowClientData, error) {
	data := &EcflowClientData{
		EcflowHost: os.Getenv("ECF_HOST"),
		EcflowPort: os.Getenv("ECF_PORT"),
		NodeName:   os.Getenv("ECF_NAME"),
		NodeRID:    os.Getenv("ECF_RID"),
		TryNo:      os.Getenv("ECF_TRYNO"),
		EcfDate:    os.Getenv("ECF_DATE"),
	}
	data.ParseCommandOptions(commandOptions)

	return data, nil
}

type EcflowClientData struct {
	Command    string              `json:"command"`
	Arguments  []string            `json:"args"`
	Envs       []map[string]string `json:"envs"`
	EcflowHost string              `json:"ecf_host"`
	EcflowPort string              `json:"ecf_port"`
	NodeName   string              `json:"ecf_name"`
	NodeRID    string              `json:"ecf_rid"`
	TryNo      string              `json:"ecf_tryno"`
	EcfDate    string              `json:"ecf_date"`
}

func (d *EcflowClientData) ParseCommandOptions(commandOptions string) {
	var opts struct {
		Host string `long:"host" description:"ecflow host"`
		Port string `long:"port" description:"ecflow port"`
	}

	parser := flags.NewParser(&opts, flags.Default)
	parser.UnknownOptionHandler = func(option string, arg flags.SplitArgument, args []string) (i []string, e error) {
		value, _ := arg.Value()
		d.Command = option
		if len(value) > 0 {
			d.Arguments = append(d.Arguments, value)
		}
		return args, nil
	}

	tokens := strings.Fields(commandOptions)

	remainArgs, err := parser.ParseArgs(tokens)
	if err != nil {
		panic(err)
	}

	d.Arguments = append(d.Arguments, remainArgs...)

	if len(opts.Host) != 0 {
		d.EcflowHost = opts.Host
	}
	if len(opts.Port) != 0 {
		d.EcflowPort = opts.Port
	}
}
