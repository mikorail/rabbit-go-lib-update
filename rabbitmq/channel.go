package rabbitmq

import (
	"sync"
	"sync/atomic"
	"time"

	debug "git.ainosi.co.id/jaklingko/lib/rabbit-go-lib-update/lib/debug"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Channel amqp.Channel wapper
type Channel struct {
	*amqp.Channel
	closed int32
	sync.Mutex
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, error) {
	c.Lock()
	ch, err := c.Connection.Channel()
	c.Unlock()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
	}

	go func() {
		for {
			// channel.Lock()
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				debug.Print("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}

			debug.Print("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(delay * time.Second)
				c.Lock()
				ch, err := c.Connection.Channel()
				c.Unlock()
				if err == nil {
					channel.Lock()
					debug.Print("channel recreate success")
					channel.Channel = ch
					channel.Unlock()
					break
				}

				debug.Printf("channel recreate failed, err: %v", err)
			}
			// channel.Unlock()
			// time.Sleep(5 * time.Second)
		}
	}()

	return channel, nil
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

// Consume warp amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			ch.Lock()
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				debug.Printf("consume failed, err: %v", err)
				time.Sleep(delay * time.Second)
				ch.Unlock()
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(delay * time.Second)

			if ch.IsClosed() {
				ch.Unlock()
				break
			}
			ch.Unlock()
		}
	}()

	return deliveries, nil
}
