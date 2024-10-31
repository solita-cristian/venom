package nats

import (
	"context"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/nats-io/nats.go"
	"github.com/ovh/venom"
	"time"
)

const Name = "nats"

type Executor struct {
	Command      string              `json:"command,omitempty" yaml:"command,omitempty"`
	Url          string              `json:"url,omitempty" yaml:"url,omitempty"`
	Subject      string              `json:"subject,omitempty" yaml:"subject,omitempty"`
	Payload      string              `json:"payload,omitempty" yaml:"payload,omitempty"`
	Header       map[string][]string `json:"header,omitempty" yaml:"header,omitempty"`
	MessageLimit int                 `json:"message_limit,omitempty" yaml:"messageLimit,omitempty"`
	Deadline     int                 `json:"deadline,omitempty" yaml:"deadline,omitempty"`
}

type Message struct {
	Data    interface{}         `json:"data,omitempty" yaml:"data,omitempty"`
	Header  map[string][]string `json:"header,omitempty" yaml:"header,omitempty"`
	Subject string              `json:"subject,omitempty" yaml:"subject,omitempty"`
}

type Result struct {
	Messages []Message `json:"messages,omitempty" yaml:"messages,omitempty"`
	Error    string    `json:"error,omitempty" yaml:"error,omitempty"`
}

func (Executor) Run(ctx context.Context, step venom.TestStep) (interface{}, error) {
	var e Executor
	if err := mapstructure.Decode(step, &e); err != nil {
		return nil, err
	}

	session, err := e.session(ctx)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	result := Result{}

	var cmdErr error
	switch e.Command {
	case "publish":
		cmdErr = e.publish(ctx, session)
		if cmdErr != nil {
			result.Error = cmdErr.Error()
		}
	case "subscribe":
		msgs, cmdErr := e.subscribe(ctx, session)
		if cmdErr != nil {
			result.Error = cmdErr.Error()
		} else {
			result.Messages = msgs
		}
	}

	return result, nil
}

func New() venom.Executor {
	return &Executor{}
}

const (
	defaultUrl           = "nats://localhost:4222"
	defaultTimeout       = 5 * time.Second
	defaultReconnectTime = 1 * time.Second
	defaultClientName    = "Venom"
)

func (e Executor) session(ctx context.Context) (*nats.Conn, error) {
	if e.Url == "" {
		venom.Warning(ctx, "No URL provided, using default %q", defaultUrl)
		e.Url = defaultUrl
	}

	opts := []nats.Option{
		nats.Timeout(defaultTimeout),
		nats.Name(defaultClientName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(defaultReconnectTime),
	}

	venom.Debug(ctx, "Connecting to NATS server %q", e.Url)

	nc, err := nats.Connect(e.Url, opts...)
	if err != nil {
		return nil, err
	}

	venom.Debug(ctx, "Connected to NATS server %q", nc.ConnectedAddr())

	return nc, nil
}

func (e Executor) publish(ctx context.Context, session *nats.Conn) error {
	if e.Subject == "" {
		return fmt.Errorf("subject is required")
	}

	venom.Debug(ctx, "Publishing message to subject %q with payload %q", e.Subject, e.Payload)

	msg := nats.Msg{
		Subject: e.Subject,
		Data:    []byte(e.Payload),
		Header:  e.Header,
	}

	err := session.PublishMsg(&msg)
	if err != nil {
		return err
	}

	venom.Debug(ctx, "Message published to subject %q", e.Subject)

	return nil
}

func (e Executor) subscribe(ctx context.Context, session *nats.Conn) ([]Message, error) {
	if e.Subject == "" {
		return nil, fmt.Errorf("subject is required")
	}

	if e.MessageLimit == 0 {
		venom.Warning(ctx, "No max messages provided, using default 1")
		e.MessageLimit = 1
	}

	if e.Deadline == 0 {
		venom.Warning(ctx, "No timeout provided, using default 5 seconds")
		e.Deadline = 5
	}

	venom.Debug(ctx, "Subscribing to subject %q", e.Subject)

	results := make([]Message, e.MessageLimit)

	ch := make(chan *nats.Msg)
	msgCount := 0
	sub, err := session.ChanSubscribe(e.Subject, ch)
	if err != nil {
		return nil, err
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(e.Deadline)*time.Second)
	defer cancel()

	venom.Debug(ctx, "Subscribed to subject %q with timeout %v and max messages %d", e.Subject, e.Deadline, e.MessageLimit)

	for {
		select {
		case msg := <-ch:
			venom.Debug(ctx, "Received message #%d from subject %q with data %q", msgCount, e.Subject, string(msg.Data))

			results[msgCount] = Message{
				Data:    string(msg.Data),
				Header:  msg.Header,
				Subject: msg.Subject,
			}

			msgCount++

			if msgCount >= e.MessageLimit {
				err = sub.Unsubscribe()
				if err != nil {
					return nil, err
				}
				return results, nil
			}
		case <-ctxWithTimeout.Done():
			_ = sub.Unsubscribe() // even it if fails, we are done anyway
			return nil, fmt.Errorf("timeout reached while waiting for message #%d from subject %q", msgCount, e.Subject)
		}
	}
}
