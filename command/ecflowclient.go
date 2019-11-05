package command

import (
	"fmt"
	"os"
	"strings"
)

func CreateEcflowClientMessage(commandOptions string) (*EcflowClientData, error) {
	// create message
	tokens := strings.Split(commandOptions, " ")
	commandToken := tokens[0]
	if commandToken[0:2] != "--" {
		return nil, fmt.Errorf("command must begin with '--': %s", commandToken)
	}

	arguments := tokens[1:]

	command := commandToken[2:]
	pos := strings.IndexByte(command, '=')
	if pos != -1 {
		arguments = append([]string{command[pos+1:]}, arguments...)
		command = command[:pos]
	}

	data := &EcflowClientData{
		Command:    command,
		Arguments:  arguments,
		EcflowHost: os.Getenv("ECF_HOST"),
		EcflowPort: os.Getenv("ECF_PORT"),
		NodeName:   os.Getenv("ECF_NAME"),
		NodeRID:    os.Getenv("ECF_RID"),
		TryNo:      os.Getenv("ECF_TRYNO"),
	}

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
}
