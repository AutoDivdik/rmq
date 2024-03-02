package rmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type RabbitMQConnStr string

func NewRabbitMQConn(connStr RabbitMQConnStr, retryTimes int64, backOffSeconds int) (*amqp.Connection, error) {
	var (
		amqpConn *amqp.Connection
		counts   int64
	)

	for {
		conn, err := amqp.Dial(string(connStr))
		if err != nil {
			counts++
		} else {
			amqpConn = conn
			break
		}

		if counts > retryTimes {
			return nil, fmt.Errorf("cannot connect to rabbitmq")
		}

		time.Sleep(time.Duration(backOffSeconds) * time.Second)
	}

	return amqpConn, nil
}
