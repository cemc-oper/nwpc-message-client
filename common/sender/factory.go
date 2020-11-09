package sender

import "time"

func CreateBrokerSender(
	brokerAddress string,
	brokerTryNo int,
	rabbitMQServer string,
	exchange string,
	routeKey string,
	writeTimeout time.Duration,
) Sender {
	rabbitmqTarget := RabbitMQTarget{
		Server:       rabbitMQServer,
		Exchange:     exchange,
		RouteKey:     routeKey,
		WriteTimeout: writeTimeout,
	}

	currentSender := BrokerSender{
		BrokerAddress: brokerAddress,
		BrokerTryNo:   brokerTryNo,
		Target:        rabbitmqTarget,
	}

	return &currentSender
}

func CreateRabbitMQSender(
	server string,
	exchange string,
	routeKey string,
	writeTimeout time.Duration) Sender {
	target := RabbitMQTarget{
		Server:       server,
		Exchange:     exchange,
		RouteKey:     routeKey,
		WriteTimeout: writeTimeout,
	}

	currentSender := RabbitMQSender{
		Target: target,
		Debug:  true,
	}

	return &currentSender
}

func CreateKafkaSender(
	brokers []string,
	topic string,
	writeTimeout time.Duration,
) Sender {
	target := KafkaTarget{
		Brokers:      brokers,
		Topic:        topic,
		WriteTimeout: writeTimeout,
	}

	currentSender := KafkaSender{
		Target: target,
		Debug:  true,
	}

	return &currentSender
}
