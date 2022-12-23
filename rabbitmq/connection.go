package rabbitmq

import (
	"sync"
	"time"

	debug "github.com/mikorail/rabbit-go-lib-update/lib/debug"
	amqp "github.com/rabbitmq/amqp091-go"
)

const delay = 3 // reconnect after delay seconds

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
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
