package publisher

import (
	"log"
	"sync"

	"git.ainosi.co.id/jaklingko/lib/rabbit-go-lib-update/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher : publisher object
type Publisher interface {
	PublishMessage(key string, message []byte, headers map[string]interface{}) error
}

type publisher struct {
	exchangeName string
	ch           *rabbitmq.Channel
	sync.Mutex
}

// NewStandartPublisher : Create new plublisher w/ aino standart (exchange topic and durable)
func NewStandartPublisher(ch *rabbitmq.Channel, exchangeName string) Publisher {
	err := ch.ExchangeDeclare(exchangeName, amqp.ExchangeTopic, true, false, false, false, nil)
	if err != nil {
		log.Panic(err)
	}
	return &publisher{
		exchangeName: exchangeName,
		ch:           ch,
	}
}

// PublishMessage : publish message with persistent mode and text/plain Content Type
func (pub *publisher) PublishMessage(key string, message []byte, headers map[string]interface{}) error {
	pub.ch.Lock()
	err := pub.ch.Publish(pub.exchangeName, key, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(message),
		Headers:      headers,
	})
	if err != nil {
		log.Printf("msg: %s, header: %v, err: %v", message, headers, err)
	}
	pub.ch.Unlock()
	return nil
}
