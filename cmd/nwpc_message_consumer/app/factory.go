package app

import "github.com/nwpc-oper/nwpc-message-client/common/consumer"

type consumerType string

const (
	printerConsumerType       consumerType = "printer"
	elasticsearchConsumerType consumerType = "elasticsearch"
)

func createPrinterConsumer(
	source consumer.RabbitMQSource,
	workerCount int,
	debug bool,
) *consumer.PrinterConsumer {
	printerConsumer := &consumer.PrinterConsumer{
		Source:      source,
		WorkerCount: workerCount,
		Debug:       debug,
	}
	return printerConsumer
}

func createElasticSearchConsumer(
	source consumer.RabbitMQSource,
	target consumer.ElasticSearchTarget,
	workerCount int,
	bulkSize int,
	debug bool,
) *consumer.ProductionConsumer {
	elasticSearchConsumer := &consumer.ProductionConsumer{
		Source:      source,
		Target:      target,
		WorkerCount: workerCount,
		BulkSize:    bulkSize,
		Debug:       debug,
	}
	return elasticSearchConsumer
}

func createEcflowClientConsumer(
	source consumer.RabbitMQSource,
	target consumer.ElasticSearchTarget,
	workerCount int,
	bulkSize int,
	debug bool,
) *consumer.EcflowClientConsumer {
	ecConsumer := &consumer.EcflowClientConsumer{
		Source:      source,
		Target:      target,
		WorkerCount: workerCount,
		BulkSize:    bulkSize,
		Debug:       debug,
	}
	return ecConsumer
}
