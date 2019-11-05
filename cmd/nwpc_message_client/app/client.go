package app

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"strings"
	"time"
)

var (
	commandOptions = ""
)

func init() {
	rootCmd.AddCommand(ecFlowClientCmd)

	ecFlowClientCmd.Flags().StringVar(&commandOptions, "command-options", "", "command options")
}

const EcflowClientMessageType = "ecflow-client"

var ecFlowClientCmd = &cobra.Command{
	Use:   "ecflow-client",
	Short: "send message for ecflow",
	Long:  "send message for ecflow",
	Run: func(cmd *cobra.Command, args []string) {
		tokens := strings.Split(commandOptions, " ")
		commandToken := tokens[0]
		if commandToken[0:2] != "--" {
			log.Fatalf("command must begin with --\n")
			return
		}

		arguments := tokens[1:]

		command := commandToken[2:]
		pos := strings.IndexByte(command, '=')
		if pos != -1 {
			arguments = append([]string{command[pos+1:]}, arguments...)
			command = command[:pos]
		}

		data := EcflowClientData{
			Command:    command,
			Arguments:  arguments,
			EcflowHost: os.Getenv("ECF_HOST"),
			EcflowPort: os.Getenv("ECF_PORT"),
			NodeName:   os.Getenv("ECF_NAME"),
			NodeRID:    os.Getenv("ECF_RID"),
			TryNo:      os.Getenv("ECF_TRYNO"),
		}

		message := EventMessage{
			App:  "nwpc-message-client",
			Type: EcflowClientMessageType,
			Time: time.Now(),
			Data: data,
		}

		messageBytes, _ := json.Marshal(message)
		fmt.Printf("%s\n", messageBytes)
	},
}

type EventMessage struct {
	App  string      `json:"app"`
	Type string      `json:"type"`
	Time time.Time   `json:"time"`
	Data interface{} `json:"data"`
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
