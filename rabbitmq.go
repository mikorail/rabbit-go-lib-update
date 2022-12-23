package rabbitmq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	debug "github.com/mikorail/go-rabbit-lib/lib/debug"
	amqp "github.com/rabbitmq/amqp091-go"
)

const delay = 3 // reconnect after delay seconds

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
	sync.Mutex
}

// Channel amqp.Channel wapper
type Channel struct {
	*amqp.Channel
	closed int32
	sync.Mutex
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				debug.Print("connection closed")
				break
			}
			debug.Printf("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(delay * time.Second)

				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Lock()
					connection.Connection = conn
					connection.Unlock()
					debug.Printf("reconnect success")
					break
				}

				debug.Printf("reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
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

			debug.Print(fmt.Sprintf("channel closed, reason: %v", reason))

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
