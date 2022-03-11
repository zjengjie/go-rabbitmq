package rabbitmq

import (
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DeliveryMode. Transient means higher throughput but messages will not be
// restored on broker restart. The delivery mode of publishings is unrelated
// to the durability of the queues they reside on. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart.
//
// This remains typed as uint8 to match Publishing.DeliveryMode. Other
// delivery modes specific to custom queue implementations are not enumerated
// here.
const (
	Transient  uint8 = amqp.Transient
	Persistent uint8 = amqp.Persistent
)

// Return captures a flattened struct of fields returned by the server when a
// Publishing is unable to be delivered either due to the `mandatory` flag set
// and no route found, or `immediate` flag set and no free consumer.
type Return struct {
	amqp.Return
}

// Confirmation notifies the acknowledgment or negative acknowledgement of a publishing identified by its delivery tag.
// Use NotifyPublish to consume these events.
type Confirmation struct {
	amqp.Confirmation
}

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	chManager *ChannelManager

	notifyReturnChan  chan Return
	notifyPublishChan chan Confirmation

	disablePublishDueToFlow    bool
	disablePublishDueToFlowMux *sync.RWMutex

	options PublisherOptions
	notify  chan error
}

// PublisherOptions are used to describe a publisher's configuration.
// Logging set to true will enable the consumer to print to stdout
type PublisherOptions struct {
	Logging           bool
	Logger            Logger
	ReconnectInterval time.Duration
}

// WithPublisherOptionsReconnectInterval sets the interval at which the publisher will
// attempt to reconnect to the rabbit server
func WithPublisherOptionsReconnectInterval(reconnectInterval time.Duration) func(options *ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.ReconnectInterval = reconnectInterval
	}
}

// WithPublisherOptionsLogging sets logging to true on the consumer options
func WithPublisherOptionsLogging(options *PublisherOptions) {
	options.Logging = true
	options.Logger = &StdLogger{}
}

// WithPublisherOptionsLogger sets logging to a custom interface.
// Use WithPublisherOptionsLogging to just log to stdout.
func WithPublisherOptionsLogger(log Logger) func(options *PublisherOptions) {
	return func(options *PublisherOptions) {
		options.Logging = true
		options.Logger = log
	}
}

// NewPublisher returns a new publisher with an open channel to the cluster.
// If you plan to enforce mandatory or immediate publishing, those failures will be reported
// on the channel of Returns that you should setup a listener on.
// Flow controls are automatically handled as they are sent from the server, and publishing
// will fail with an error when the server is requesting a slowdown
func NewPublisher(url string, config amqp.Config, optionFuncs ...func(*PublisherOptions)) (*Publisher, error) {
	options := &PublisherOptions{
		Logging:           true,
		Logger:            &StdLogger{},
		ReconnectInterval: time.Second * 5,
	}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	chManager, err := NewChannelManager(url, config, options.Logger, options.ReconnectInterval)
	if err != nil {
		return nil, err
	}

	notify := make(chan error)
	chManager.addNotify(notify)
	publisher := &Publisher{
		chManager:                  chManager,
		disablePublishDueToFlow:    false,
		disablePublishDueToFlowMux: &sync.RWMutex{},
		options:                    *options,
		notifyReturnChan:           nil,
		notifyPublishChan:          nil,
		notify:                     notify,
	}

	go publisher.startNotifyFlowHandler()

	go publisher.handleRestarts()

	return publisher, nil
}

func NewPublisherWithChannel(chManager *ChannelManager, optionFuncs ...func(*PublisherOptions)) (*Publisher, error) {
	options := &PublisherOptions{
		Logging:           true,
		Logger:            chManager.Logger,
		ReconnectInterval: chManager.ReconnectInterval,
	}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	notify := make(chan error)
	chManager.addNotify(notify)
	publisher := &Publisher{
		chManager:                  chManager,
		disablePublishDueToFlow:    false,
		disablePublishDueToFlowMux: &sync.RWMutex{},
		options:                    *options,
		notifyReturnChan:           nil,
		notifyPublishChan:          nil,
		notify:                     notify,
	}

	go publisher.startNotifyFlowHandler()

	go publisher.handleRestarts()

	return publisher, nil
}

func (publisher *Publisher) handleRestarts() {
	for err := range publisher.notify {
		publisher.options.Logger.Printf("successful publisher recovery from: %v", err)
		go publisher.startNotifyFlowHandler()
		if publisher.notifyReturnChan != nil {
			go publisher.startNotifyReturnHandler()
		}
		if publisher.notifyPublishChan != nil {
			go publisher.startNotifyPublishHandler()
		}
	}
}

// NotifyReturn registers a listener for basic.return methods.
// These can be sent from the server when a publish is undeliverable either from the mandatory or immediate flags.
func (publisher *Publisher) NotifyReturn() <-chan Return {
	publisher.notifyReturnChan = make(chan Return)
	go publisher.startNotifyReturnHandler()
	return publisher.notifyReturnChan
}

// NotifyPublish registers a listener for publish confirmations, must set ConfirmPublishings option
func (publisher *Publisher) NotifyPublish() <-chan Confirmation {
	publisher.notifyPublishChan = make(chan Confirmation)
	go publisher.startNotifyPublishHandler()
	return publisher.notifyPublishChan
}

// Publish publishes the provided data to the given routing keys over the connection
func (publisher *Publisher) Publish(
	data []byte,
	routingKeys []string,
	optionFuncs ...func(*PublishOptions),
) error {
	publisher.disablePublishDueToFlowMux.RLock()
	if publisher.disablePublishDueToFlow {
		return fmt.Errorf("publishing blocked due to high flow on the server")
	}
	publisher.disablePublishDueToFlowMux.RUnlock()

	options := &PublishOptions{}
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	for _, routingKey := range routingKeys {
		var message = amqp.Publishing{}
		message.ContentType = options.ContentType
		message.DeliveryMode = options.DeliveryMode
		message.Body = data
		message.Headers = tableToAMQPTable(options.Headers)
		message.Expiration = options.Expiration
		message.ContentEncoding = options.ContentEncoding
		message.Priority = options.Priority
		message.CorrelationId = options.CorrelationID
		message.ReplyTo = options.ReplyTo
		message.MessageId = options.MessageID
		message.Timestamp = options.Timestamp
		message.Type = options.Type
		message.UserId = options.UserID
		message.AppId = options.AppID

		// Actual publish.
		err := publisher.chManager.channel.Publish(
			options.Exchange,
			routingKey,
			options.Mandatory,
			options.Immediate,
			message,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// StopPublishing stops the publishing of messages.
// The publisher should be discarded as it's not safe for re-use
func (publisher Publisher) StopPublishing() error {
	err := publisher.chManager.close()
	if err != nil {
		return err
	}
	return nil
}

func (publisher *Publisher) startNotifyFlowHandler() {
	notifyFlowChan := publisher.chManager.channel.NotifyFlow(make(chan bool))
	publisher.disablePublishDueToFlowMux.Lock()
	publisher.disablePublishDueToFlow = false
	publisher.disablePublishDueToFlowMux.Unlock()

	// Listeners for active=true flow control.  When true is sent to a listener,
	// publishing should pause until false is sent to listeners.
	for ok := range notifyFlowChan {
		publisher.disablePublishDueToFlowMux.Lock()
		if ok {
			publisher.options.Logger.Printf("pausing publishing due to flow request from server")
			publisher.disablePublishDueToFlow = true
		} else {
			publisher.disablePublishDueToFlow = false
			publisher.options.Logger.Printf("resuming publishing due to flow request from server")
		}
		publisher.disablePublishDueToFlowMux.Unlock()
	}
}

func (publisher *Publisher) startNotifyReturnHandler() {
	returnAMQPCh := publisher.chManager.channel.NotifyReturn(make(chan amqp.Return, 1))
	for ret := range returnAMQPCh {
		publisher.notifyReturnChan <- Return{ret}
	}
}

func (publisher *Publisher) startNotifyPublishHandler() {
	publisher.chManager.channel.Confirm(false)
	publishAMQPCh := publisher.chManager.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	for conf := range publishAMQPCh {
		publisher.notifyPublishChan <- Confirmation{conf}
	}
}
