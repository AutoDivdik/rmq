package rmq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

type workerFn func(ctx context.Context, messages <-chan amqp.Delivery)

type EventConsumer interface {
	Consume(fn workerFn) error
}
