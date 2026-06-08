package queue

import (
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultSubjectQueueSize  = 10240
	defaultPublishAckTimeout = 2 * time.Second
	defaultAckWorkerCount    = 8
	defaultAckQueueSize      = 4096
)

type queueConfig struct {
	subjectQueueSize  int
	publishAckTimeout time.Duration
	ackWorkerCount    int
	ackQueueSize      int
	natsOptions       []nats.Option
}

func defaultQueueConfig() queueConfig {
	return queueConfig{
		subjectQueueSize:  defaultSubjectQueueSize,
		publishAckTimeout: defaultPublishAckTimeout,
		ackWorkerCount:    defaultAckWorkerCount,
		ackQueueSize:      defaultAckQueueSize,
	}
}

type QueueOption func(*queueConfig)

func WithSubjectQueueSize(size int) QueueOption {
	return func(cfg *queueConfig) {
		if size > 0 {
			cfg.subjectQueueSize = size
		}
	}
}

func WithPublishAckTimeout(timeout time.Duration) QueueOption {
	return func(cfg *queueConfig) {
		if timeout > 0 {
			cfg.publishAckTimeout = timeout
		}
	}
}

func WithAckWorkerCount(count int) QueueOption {
	return func(cfg *queueConfig) {
		if count > 0 {
			cfg.ackWorkerCount = count
		}
	}
}

func WithAckQueueSize(size int) QueueOption {
	return func(cfg *queueConfig) {
		if size > 0 {
			cfg.ackQueueSize = size
		}
	}
}

func WithNatsOptions(natsOptions ...nats.Option) QueueOption {
	return func(cfg *queueConfig) {
		if len(natsOptions) > 0 {
			cfg.natsOptions = natsOptions
		}
	}
}

func applyQueueOptions(options []QueueOption) queueConfig {
	cfg := defaultQueueConfig()
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	return cfg
}
