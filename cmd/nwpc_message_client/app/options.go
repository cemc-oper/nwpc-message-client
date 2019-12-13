package app

import "time"

var (
	rabbitmqServer = ""
	disableSend    = false

	commandOptions = ""

	system         = ""
	productionType = ""
	event          = ""
	status         = ""
	startTime      = ""
	forecastTime   = ""

	brokerAddress  = ""
	useBroker      = true
	disableDeliver = false

	writeTimeOut = 2 * time.Second

	Version   = "Unknown version"
	BuildTime = "Unknown build time"
	GitCommit = "Unknown GitCommit"
)
