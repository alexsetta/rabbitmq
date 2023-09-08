package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"rabbitmq/rabbitmq"
	"time"
)

func main() {
	conn, ch, err := rabbitmq.NewConsumer("amqp://guest:guest@localhost:5672/rabbit", 10)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	defer ch.Close()

	maxWorkers := 100
	done := make(chan bool, maxWorkers)
	err = rabbitmq.MultiProcess(ch, done, "fila.teste", "consumer", maxWorkers, worker)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	forever := make(chan bool)
	<-forever
}

func worker(ch *amqp.Channel, done chan bool, queue, consumer string) error {
	msgs, err := ch.Consume(queue, consumer, false, false, false, false, nil)
	if err != nil {
		return err
	}

	for msg := range msgs {
		log.Println("Processing message", msg.MessageId, string(msg.Body))
		time.Sleep(1 * time.Second)
		msg.Ack(false)
	}
	done <- true

	return nil
}
