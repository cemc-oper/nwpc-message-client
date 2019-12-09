package app

import "time"

var (
	brokerAddress  = ""
	useBroker      = true
	disableDeliver = false
	commandOptions = ""
	rabbitmqServer = ""

	system         = ""
	productionType = ""
	event          = ""
	status         = ""
	startTime      = ""
	forecastTime   = ""

	writeTimeOut = 2 * time.Second

	Version   = "Unknown version"
	BuildTime = "Unknown build time"
	GitCommit = "Unknown GitCommit"
)
