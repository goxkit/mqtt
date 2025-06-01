// Copyright (c) 2025, The GoKit Authors
// MIT License
// All rights reserved.

package mqtt

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"fmt"

	myQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/goxkit/logging"
	"github.com/goxkit/messaging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type (
	subscription struct {
		qos     QoS
		topic   string
		handler messaging.ConsumerHandler
	}

	// mqttDispatcher is the concrete implementation of the Dispatcher interface.
	mqttDispatcher struct {
		logger      logging.Logger
		client      myQTT.Client
		subscribers []*subscription
		signalCh    chan os.Signal
		tracer      trace.Tracer
	}
)

// NewDispatcher initializes a new mqttDispatcher with the provided logger and MQTT client.
func NewDispatcher(logger logging.Logger, client myQTT.Client) messaging.Dispatcher {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	return &mqttDispatcher{
		logger:      logger,
		client:      client,
		subscribers: []*subscription{},
		signalCh:    signalCh,
		tracer:      otel.Tracer("gokit/mqtt"),
	}
}

func (d *mqttDispatcher) Register(from string, msgType any, handler messaging.ConsumerHandler, options ...messaging.DispatcherOption) error {
	if from == "" {
		return EmptyTopicError
	}

	if handler == nil {
		return NillHandlerError
	}

	d.subscribers = append(d.subscribers, &subscription{0, from, handler})

	return nil
}

func (d *mqttDispatcher) ConsumeBlocking() {
	for _, s := range d.subscribers {
		d.logger.Debug(LogMessage("subscribing to topic: ", s.topic))
		d.client.Subscribe(s.topic, 1, d.defaultMessageHandler(s.handler))
	}

	<-d.signalCh

	d.logger.Warn(LogMessage("received stop signal, unsubscribing..."))

	for _, s := range d.subscribers {
		d.logger.Warn(LogMessage("unsubscribing to topic: ", s.topic))
		d.client.Unsubscribe(s.topic)
	}

	d.logger.Debug(LogMessage("stopping consumer..."))
}

// defaultMessageHandler wraps a Handler with additional functionality, such as tracing.
func (d *mqttDispatcher) defaultMessageHandler(handler messaging.ConsumerHandler) myQTT.MessageHandler {
	return func(_ myQTT.Client, msg myQTT.Message) {
		d.logger.Debug(LogMessage("received message from topic: ", msg.Topic()))
		msg.Ack()

		// Create a new context with an OpenTelemetry span using the dispatcher tracer.
		ctx, span := d.tracer.Start(context.Background(), msg.Topic())
		defer span.End()

		metadata := map[string]string{}
		metadata["topic"] = msg.Topic()
		metadata["qos"] = string(msg.Qos())
		metadata["message_id"] = fmt.Sprint(msg.MessageID())

		err := handler(ctx, msg.Payload(), metadata)
		if err != nil {
			d.logger.Error(LogMessage("failure to execute the topic handler"), zap.Error(err))
		}

		d.logger.Debug(LogMessage("message processed successfully"))
	}
}
