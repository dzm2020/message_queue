package queue

import (
	"time"

	"github.com/nats-io/nats.go"
)

const (
	defaultPublishAckTimeout = 2 * time.Second
)

type queueConfig struct {
	publishAckTimeout time.Duration
	natsOptions       []nats.Option
	logger            Logger
	enableDebugLog    bool
}

func defaultQueueConfig() queueConfig {
	return queueConfig{
		publishAckTimeout: defaultPublishAckTimeout,
		logger:            defaultLogger(),
		enableDebugLog:    false,
	}
}

type QueueOption func(*queueConfig)

func WithPublishAckTimeout(timeout time.Duration) QueueOption {
	return func(cfg *queueConfig) {
		if timeout > 0 {
			cfg.publishAckTimeout = timeout
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

// WithLogger 配置外部注入日志器。
func WithLogger(logger Logger) QueueOption {
	return func(cfg *queueConfig) {
		if logger != nil {
			cfg.logger = logger
		}
	}
}

// WithDebugLogEnabled 控制是否输出 Debug 级别日志。
func WithDebugLogEnabled(enabled bool) QueueOption {
	return func(cfg *queueConfig) {
		cfg.enableDebugLog = enabled
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
