package rmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	_exchangeKind       = "direct"
	_exchangeDurable    = true
	_exchangeAutoDelete = false
	_exchangeInternal   = false
	_exchangeNoWait     = false

	_queueDurable    = true
	_queueAutoDelete = false
	_queueExclusive  = false
	_queueNoWait     = false

	_prefetchCount  = 5
	_prefetchSize   = 0
	_prefetchGlobal = false

	_consumeAutoAck   = false
	_consumeExclusive = false
	_consumeNoLocal   = false
	_consumeNoWait    = false

	_exchangeName   = "app-exchange"
	_queueName      = "app-queue"
	_bindingKey     = "app-routing-key"
	_consumerTag    = "app-consumer"
	_workerPoolSize = 24
)

type Consumer struct {
	conn *amqp.Connection

	exchangeName, queueName, bindingKey, consumerTag string
	workerPoolSize                                   int
}

func NewConsumer(amqpConn *amqp.Connection) (EventConsumer, error) {
	cons := &Consumer{
		conn:           amqpConn,
		exchangeName:   _exchangeName,
		queueName:      _queueName,
		bindingKey:     _bindingKey,
		consumerTag:    _consumerTag,
		workerPoolSize: _workerPoolSize,
	}

	return cons, nil
}
func (q *Consumer) Consume(fn workerFn) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := q.createChannel()
	if err != nil {
		return fmt.Errorf("error while open channel: %v", err)
	}

	messages, err := ch.Consume(
		q.queueName,
		q.consumerTag,
		_consumeAutoAck,
		_consumeExclusive,
		_consumeNoLocal,
		_consumeNoWait,
		nil)
	if err != nil {
		return fmt.Errorf("consumer error")
	}

	inf := make(chan struct{})

	for i := 0; i < q.workerPoolSize; i++ {
		go fn(ctx, messages)
	}

	errChan := <-ch.NotifyClose(make(chan *amqp.Error))
	<-inf

	return errChan
}

func (q *Consumer) createChannel() (*amqp.Channel, error) {
	ch, err := q.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open the channel")
	}

	if err := ch.ExchangeDeclare(
		q.exchangeName,
		_exchangeKind,
		_exchangeDurable,
		_exchangeAutoDelete,
		_exchangeInternal,
		_exchangeNoWait,
		nil); err != nil {
		return nil, fmt.Errorf("failed to declare exchange")
	}

	queue, err := ch.QueueDeclare(
		q.queueName,
		_queueDurable,
		_queueAutoDelete,
		_queueExclusive,
		_queueNoWait,
		nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue")
	}

	if err := ch.QueueBind(
		queue.Name,
		q.bindingKey,
		q.exchangeName,
		_queueNoWait,
		nil); err != nil {
		return nil, fmt.Errorf("failed to bind queue")
	}

	if err := ch.Qos(_prefetchCount, _prefetchSize, _prefetchGlobal); err != nil {
		return nil, fmt.Errorf("failed to QOS")
	}

	return ch, nil
}
