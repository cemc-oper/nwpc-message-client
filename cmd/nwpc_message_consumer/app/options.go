package app

var (
	rabbitmqServer    = ""
	rabbitmqQueueName = ""
	elasticServer     = ""
	workerCount       = 2
	bulkSize          = 20
	isDebug           = true
	consumerType      = "print"

	Version   = "Unknown Version"
	BuildTime = "Unknown BuildTime"
	GitCommit = "Unknown GitCommit"
)
