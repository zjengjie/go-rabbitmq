package rabbitmq

import (
	"fmt"
	"log"
)

// Logger is the interface to send logs to. It can be set using
// WithPublisherOptionsLogger() or WithConsumerOptionsLogger().
type Logger interface {
	Printf(string, ...interface{})
}

const loggingPrefix = "gorabbit"

// StdLogger logs to stdout using go's default Logger.
type StdLogger struct{}

func (l StdLogger) Printf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s: %s", loggingPrefix, format), v...)
}
