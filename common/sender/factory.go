package sender

import "time"

func CreateBrokerSender(
	brokerAddress string,
	rabbitMQServer string,
	exchange string,
	routeKey string,
	writeTimeout time.Duration) Sender {
	rabbitmqTarget := RabbitMQTarget{
		Server:       rabbitMQServer,
		Exchange:     exchange,
		RouteKey:     routeKey,
		WriteTimeout: writeTimeout,
	}

	brokerSender := BrokerSender{
		BrokerAddress: brokerAddress,
		Target:        rabbitmqTarget,
	}

	return &brokerSender
}

func CreateRabbitMQSender(
	server string,
	exchange string,
	routeKey string,
	writeTimeout time.Duration) Sender {
	rabbitmqTarget := RabbitMQTarget{
		Server:       server,
		Exchange:     exchange,
		RouteKey:     routeKey,
		WriteTimeout: writeTimeout,
	}

	rabbitSender := RabbitMQSender{
		Target: rabbitmqTarget,
		Debug:  true,
	}

	return &rabbitSender
}
