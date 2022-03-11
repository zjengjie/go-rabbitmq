package rabbitmq

import (
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ChannelManager struct {
	Logger              Logger
	url                 string
	channel             *amqp.Channel
	connection          *amqp.Connection
	amqpConfig          amqp.Config
	channelMux          *sync.RWMutex
	notifyCancelOrClose []chan error
	ReconnectInterval   time.Duration
}

func NewChannelManager(url string, conf amqp.Config, log Logger, reconnectInterval time.Duration) (*ChannelManager, error) {
	conn, ch, err := getNewChannel(url, conf)
	if err != nil {
		return nil, err
	}

	chManager := ChannelManager{
		Logger:              log,
		url:                 url,
		connection:          conn,
		channel:             ch,
		channelMux:          &sync.RWMutex{},
		amqpConfig:          conf,
		notifyCancelOrClose: make([]chan error, 0),
		ReconnectInterval:   reconnectInterval,
	}
	go chManager.startNotifyCancelOrClosed()
	return &chManager, nil
}

func (chManager *ChannelManager) addNotify(notify chan error) {
	chManager.channelMux.Lock()
	defer chManager.channelMux.Unlock()
	chManager.notifyCancelOrClose = append(chManager.notifyCancelOrClose, notify)
}

func getNewChannel(url string, conf amqp.Config) (*amqp.Connection, *amqp.Channel, error) {
	amqpConn, err := amqp.DialConfig(url, conf)
	if err != nil {
		return nil, nil, err
	}
	ch, err := amqpConn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return amqpConn, ch, nil
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (chManager *ChannelManager) startNotifyCancelOrClosed() {
	notifyCloseChan := chManager.channel.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := chManager.channel.NotifyCancel(make(chan string, 1))

	select {
	case err := <-notifyCloseChan:
		// If the connection close is triggered by the Server, a reconnection takes place
		if err != nil {
			chManager.Logger.Printf("attempting to reconnect to amqp server after close")
			chManager.reconnectLoop()
			chManager.Logger.Printf("successfully reconnected to amqp server after close")
			chManager.notifyAll(err)
		}
		if err == nil {
			chManager.Logger.Printf("amqp channel closed gracefully")
		}
	case err := <-notifyCancelChan:
		chManager.Logger.Printf("attempting to reconnect to amqp server after cancel")
		chManager.reconnectLoop()
		chManager.Logger.Printf("successfully reconnected to amqp server after cancel")
		chManager.notifyAll(errors.New(err))
	}
}

func (chManager *ChannelManager) notifyAll(err error) {
	for _, notify := range chManager.notifyCancelOrClose {
		notify <- err
	}
}

// reconnectLoop continuously attempts to reconnect
func (chManager *ChannelManager) reconnectLoop() {
	for {
		chManager.Logger.Printf("waiting %s seconds to attempt to reconnect to amqp server", chManager.ReconnectInterval)
		time.Sleep(chManager.ReconnectInterval)
		err := chManager.reconnect()
		if err != nil {
			chManager.Logger.Printf("error reconnecting to amqp server: %v", err)
		} else {
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (chManager *ChannelManager) reconnect() error {
	chManager.channelMux.Lock()
	defer chManager.channelMux.Unlock()
	newConn, newChannel, err := getNewChannel(chManager.url, chManager.amqpConfig)
	if err != nil {
		return err
	}

	chManager.channel.Close()
	chManager.connection.Close()

	chManager.connection = newConn
	chManager.channel = newChannel
	go chManager.startNotifyCancelOrClosed()
	return nil
}

// close safely closes the current channel
func (chManager *ChannelManager) close() error {
	chManager.channelMux.Lock()
	defer chManager.channelMux.Unlock()

	err := chManager.connection.Close()
	if err != nil {
		return err
	}
	return nil
}
