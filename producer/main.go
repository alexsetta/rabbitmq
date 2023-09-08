package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/rabbit")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	confirms := make(chan amqp.Confirmation)
	ch.NotifyPublish(confirms)
	go func() {
		for confirm := range confirms {
			if confirm.Ack {
				// code when messages is confirmed
				log.Printf("Confirmed")
			} else {
				// code when messages is nack-ed
				log.Printf("Nacked")
			}
		}
	}()

	err = ch.Confirm(false)
	failOnError(err, "Failed to confirm")

	q, err := ch.QueueDeclare("fila.teste", false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	ctx := context.Background()

	numMessages := 0
	ticker := time.NewTicker(1 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			numMessages++
			body := fmt.Sprintf("Mensagem #%d", numMessages)
			err = ch.PublishWithContext(
				ctx,
				"",     // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(body),
					MessageId:   fmt.Sprintf("id%d", numMessages),
				})
			failOnError(err, "Failed to publish a message")
			log.Printf(" [x] Sent %s", body)
		}
	}
}
