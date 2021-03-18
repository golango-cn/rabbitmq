package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {

	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", "localhost", 5672))
	if err != nil {
		fmt.Println("connect rabbitmq server failed:", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("create channel failed:", err)
		return
	}
	defer ch.Close()

	err = ch.ExchangeDeclare("exchange.dlx", "direct", true, false, false, false, nil)
	if err != nil {
		fmt.Println("declare dlx exchange error:", err)
		return
	}

	err = ch.ExchangeDeclare("exchange.normal", "fanout", false, true, false, false, nil)
	if err != nil {
		fmt.Println("declare normal exchange error:", err)
		return
	}

	var (
		args = make(map[string]interface{})
	)
	//设置队列的过期时间
	args["x-message-ttl"] = 600000
	//设置死信交换器
	args["x-dead-letter-exchange"] = "exchange.dlx"
	//设置死信交换器Key
	args["x-dead-letter-routing-key"] = "dlxKey"
	normalQueue, err := ch.QueueDeclare("queue.normal", false, false, false, true, args)
	if err != nil {
		fmt.Println("declare normal queue error:", err)
		return
	}
	err = ch.QueueBind(normalQueue.Name, "normalKey", "exchange.normal", false, nil)
	if err != nil {
		fmt.Println("bind normal queue error:", err)
		return
	}

	dlxQueue, err := ch.QueueDeclare("queue.dlx", false, false, false, true, nil)
	if err != nil {
		fmt.Println("declare dlx queue error:", err)
		return
	}
	err = ch.QueueBind(dlxQueue.Name, "dlxKey", "exchange.dlx", false, nil)
	if err != nil {
		fmt.Println("bind dlx queue error:", err)
		return
	}

	deliveries, _ := ch.Consume("queue.normal", "tag1", false, false, false, false, nil)
	go handle(deliveries)

	for count := 0; count < 20; count++ {
		msg := fmt.Sprintf("test dlx message .%d", count)
		ch.Publish("exchange.normal", "normalKey", false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
			Expiration:  "5000",
		})
	}

	time.Sleep(10 * time.Second)

}

func handle(deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		log.Printf(
			"got %dB delivery: [%v] %q",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)

		// d.Acknowledger.Ack()

		// err := d.Acknowledger.Reject(d.DeliveryTag, true)
		// err := d.Ack(false)
		// fmt.Println(err)
	}
}
