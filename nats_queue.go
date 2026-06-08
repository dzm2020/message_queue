package queue

import (
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var ErrNilSubscriber = errors.New("subscriber is nil")
var ErrNilConnection = errors.New("nats connection is nil")
var ErrSubjectAlreadySubscribed = errors.New("subject already subscribed")
var ErrPublishAckQueueFull = errors.New("publish ack queue is full")

// NewNATSMessageQueue 创建并连接 NATS 消息队列实例。
func NewNATSMessageQueue(url string, queueOptions ...QueueOption) (IMessageQue, error) {
	cfg := applyQueueOptions(queueOptions)
	conn, err := nats.Connect(url, cfg.natsOptions...)
	if err != nil {
		return nil, err
	}
	return newNATSMessageQueueFromConn(conn, cfg), nil
}

// NewNATSMessageQueueFromConn 使用现有连接创建消息队列实例。
func NewNATSMessageQueueFromConn(conn *nats.Conn) IMessageQue {
	return NewNATSMessageQueueFromConnWithOptions(conn)
}

// NewNATSMessageQueueFromConnWithOptions 使用现有连接创建消息队列实例，并支持队列配置项。
func NewNATSMessageQueueFromConnWithOptions(conn *nats.Conn, queueOptions ...QueueOption) IMessageQue {
	return newNATSMessageQueueFromConn(conn, applyQueueOptions(queueOptions))
}

func newNATSMessageQueueFromConn(conn *nats.Conn, cfg queueConfig) *natsMessageQueue {
	mq := &natsMessageQueue{
		conn:          conn,
		cfg:           cfg,
		dispatchers:   make(map[string]*subjectDispatcher),
		subscriptions: make(map[string]*nats.Subscription),
	}
	if conn != nil {
		mq.ack = newAckWorkerPool(conn, cfg.ackWorkerCount, cfg.ackQueueSize, cfg.publishAckTimeout, &mq.connStats)
	}
	mq.installConnectionHandlers()
	return mq
}

type natsMessageQueue struct {
	conn *nats.Conn
	ack  *ackWorkerPool

	cfg           queueConfig
	connStats     connectionEventStats
	mu            sync.RWMutex
	dispatchers   map[string]*subjectDispatcher
	subscriptions map[string]*nats.Subscription
}

type natsSubscription struct {
	sub     *nats.Subscription
	cleanup func()
	once    sync.Once
}

func (mq *natsMessageQueue) Publish(subject string, data []byte) error {
	mq.mu.RLock()
	ack := mq.ack
	mq.mu.RUnlock()

	if ack == nil {
		return ErrNilConnection
	}
	if err := ack.Enqueue(subject, data); err != nil {
		return err
	}
	return nil
}

func (mq *natsMessageQueue) Request(subject string, data []byte, timeout time.Duration) ([]byte, error) {
	mq.mu.RLock()
	conn := mq.conn
	mq.mu.RUnlock()
	if conn == nil {
		return nil, ErrNilConnection
	}
	msg, err := conn.Request(subject, data, timeout)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (mq *natsMessageQueue) Subscribe(subject string, subscriber ISubscriber) (ISubscription, error) {
	if subscriber == nil {
		return nil, ErrNilSubscriber
	}

	mq.mu.Lock()
	conn := mq.conn
	if conn == nil {
		mq.mu.Unlock()
		return nil, ErrNilConnection
	}
	if _, exists := mq.dispatchers[subject]; exists {
		mq.mu.Unlock()
		return nil, ErrSubjectAlreadySubscribed
	}

	dispatcher := newSubjectDispatcher(subject, subscriber, mq.cfg.subjectQueueSize, &mq.connStats)

	mq.dispatchers[subject] = dispatcher
	mq.mu.Unlock()

	sub, err := conn.ChanSubscribe(subject, dispatcher.msgCh)
	if err != nil {
		mq.mu.Lock()
		delete(mq.dispatchers, subject)
		mq.mu.Unlock()
		dispatcher.stopAndWait()
		return nil, err
	}

	mq.mu.Lock()
	if mq.conn == nil {
		mq.mu.Unlock()
		_ = sub.Unsubscribe()
		dispatcher.stopAndWait()
		return nil, ErrNilConnection
	}
	mq.subscriptions[subject] = sub
	mq.mu.Unlock()

	cleanup := func() {
		mq.mu.Lock()
		subscription, subExists := mq.subscriptions[subject]
		if subExists && subscription == sub {
			delete(mq.subscriptions, subject)
		}
		current, exists := mq.dispatchers[subject]
		if exists && current == dispatcher {
			delete(mq.dispatchers, subject)
		}
		mq.mu.Unlock()
		dispatcher.stopAndWait()
	}

	return &natsSubscription{sub: sub, cleanup: cleanup}, nil
}

func (s *natsSubscription) Unsubscribe() error {
	var unsubscribeErr error
	s.once.Do(func() {
		unsubscribeErr = s.sub.Unsubscribe()
		if s.cleanup != nil {
			s.cleanup()
		}
	})
	return unsubscribeErr
}

// Close 关闭 NATS 连接。
func (mq *natsMessageQueue) Close() {
	mq.mu.Lock()
	dispatchers := make([]*subjectDispatcher, 0, len(mq.dispatchers))
	subscriptions := make([]*nats.Subscription, 0, len(mq.subscriptions))
	for _, dispatcher := range mq.dispatchers {
		dispatchers = append(dispatchers, dispatcher)
	}
	for _, sub := range mq.subscriptions {
		subscriptions = append(subscriptions, sub)
	}
	mq.dispatchers = make(map[string]*subjectDispatcher)
	mq.subscriptions = make(map[string]*nats.Subscription)
	conn := mq.conn
	ack := mq.ack
	mq.conn = nil
	mq.ack = nil
	mq.mu.Unlock()

	for _, sub := range subscriptions {
		_ = sub.Unsubscribe()
	}

	for _, dispatcher := range dispatchers {
		dispatcher.stopAndWait()
	}

	if ack != nil {
		ack.Close()
	}

	if conn != nil {
		conn.Close()
	}
}

// ConnectionEventStats 返回断连/重连统计快照。
func (mq *natsMessageQueue) ConnectionEventStats() ConnectionEventStats {
	return mq.connStats.snapshot()
}

func (mq *natsMessageQueue) installConnectionHandlers() {
	if mq.conn == nil {
		return
	}

	mq.conn.SetDisconnectErrHandler(func(conn *nats.Conn, err error) {
		mq.connStats.onDisconnect(err)
	})

	mq.conn.SetReconnectHandler(func(conn *nats.Conn) {
		mq.connStats.onReconnect()
	})
}
