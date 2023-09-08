package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strconv"
)

type Worker func(ch *amqp.Channel, done chan bool, queue, consumer string) error

func NewConsumer(url string, fetchCount int) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	err = channel.Qos(fetchCount, 0, false)
	if err != nil {
		return nil, nil, err
	}

	return conn, channel, nil
}

func MultiProcess(ch *amqp.Channel, done chan bool, queue, consumer string, maxWorkers int, worker Worker) error {
	for i := 0; i < maxWorkers; i++ {
		go func(i int) {
			err := worker(ch, done, queue, consumer+"-"+strconv.Itoa(i))
			if err != nil {
				log.Println(err)
			}
		}(i)
	}

	return nil
}

func OpenChannel(url string) (*amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func Consume(ch *amqp.Channel, queue, consumer string, out chan amqp.Delivery) error {
	q, err := ch.QueueDeclare(queue, false, false, false, false, nil)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(q.Name, consumer, false, false, false, false, nil)
	if err != nil {
		return err
	}

	for msg := range msgs {
		out <- msg
	}

	return nil
}
