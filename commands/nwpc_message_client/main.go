package main

import (
	"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_client/app"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})
}

func main() {
	app.Execute()
}
