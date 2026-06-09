package queue

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"github.com/nats-io/nats.go"
)

const publishRequestHeader = "X-MQ-Publish"

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
		conn:    conn,
		cfg:     cfg,
		waiters: maputil.NewConcurrentMap[int64, *nats.Msg](32),
	}
	mq.debugf("queue init start has_conn=%t ack_timeout=%s", conn != nil, cfg.publishAckTimeout)
	mq.connStats.logger = cfg.logger
	mq.installConnectionHandlers()
	if err := mq.initWaiters(); err != nil && mq.cfg.logger != nil {
		mq.cfg.logger.Errorf("queue init waiters failed err=%v", err)
	}
	mq.debugf("queue init done waiter_inbox=%s", mq.waiterInbox)
	return mq
}

type natsMessageQueue struct {
	conn *nats.Conn

	cfg         queueConfig
	connStats   connectionEventStats
	waiters     *maputil.ConcurrentMap[int64, *nats.Msg]
	waiterSeq   atomic.Int64
	waiterInbox string
}

func (mq *natsMessageQueue) debugf(format string, args ...any) {
	if mq != nil && mq.cfg.logger != nil {
		mq.cfg.logger.Debugf(format, args...)
	}
}

func (mq *natsMessageQueue) Publish(subject string, data []byte) error {
	logger := mq.cfg.logger
	waiterID := mq.waiterSeq.Add(1)
	mq.debugf("publish start subject=%s bytes=%d waiter_id=%d", subject, len(data), waiterID)

	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header.Set(publishRequestHeader, "1")
	msg.Reply = mq.waiterReplySubject(waiterID)

	mq.addWaiter(waiterID, msg)
	if err := mq.conn.PublishMsg(msg); err != nil {
		mq.delWaiter(waiterID)
		logger.Errorf("queue publish enqueue ack failed subject=%s err=%v", subject, err)
		return err
	}

	mq.debugf("publish sent subject=%s waiter_id=%d reply=%s", subject, waiterID, msg.Reply)
	return nil
}

func (mq *natsMessageQueue) Request(subject string, data []byte, timeout time.Duration) ([]byte, error) {
	conn := mq.conn
	logger := mq.cfg.logger
	mq.debugf("request start subject=%s bytes=%d timeout=%s", subject, len(data), timeout)
	msg, err := conn.Request(subject, data, timeout)
	if err != nil {
		logger.Errorf("queue request err subject=%s timeout=%s err=%v", subject, timeout, err)
		return nil, err
	}
	mq.debugf("request done subject=%s reply_bytes=%d", subject, len(msg.Data))
	return msg.Data, nil
}

func (mq *natsMessageQueue) Subscribe(subject string, subscriber ISubscriber) (ISubscription, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber is nil")
	}
	mq.debugf("subscribe start subject=%s", subject)
	return mq.conn.Subscribe(subject, func(msg *nats.Msg) {
		mq.handlerMessage(subject, subscriber, msg)
	})
}

func (mq *natsMessageQueue) handlerMessage(subject string, subscriber ISubscriber, msg *nats.Msg) {
	defer func() {
		if r := recover(); r != nil {
			mq.connStats.onDispatcherPanic()
		}
	}()

	isPublishMessage := msg.Header.Get(publishRequestHeader) == "1"
	isSync := !isPublishMessage
	mq.debugf("message received subject=%s is_sync=%t bytes=%d has_reply=%t", subject, isSync, len(msg.Data), msg.Reply != "")
	var once sync.Once
	response := func(data []byte) error {
		var err error
		once.Do(func() {
			if msg.Reply == "" {
				return
			}
			err = msg.Respond(data)
		})
		return err
	}
	if !isSync {
		_ = response(nil)
	}
	subscriber.OnMessage(msg.Data, isSync, response)
}

func (mq *natsMessageQueue) ConnectionEventStats() ConnectionEventStats {
	return mq.connStats.snapshot()
}

func (mq *natsMessageQueue) installConnectionHandlers() {
	if mq.conn == nil {
		return
	}

	mq.conn.SetDisconnectErrHandler(func(conn *nats.Conn, err error) {
		mq.connStats.onDisconnect(err)
		mq.debugf("connection event disconnected err=%v", err)
	})

	mq.conn.SetReconnectHandler(func(conn *nats.Conn) {
		mq.connStats.onReconnect()
		mq.debugf("connection event reconnected server=%s", conn.ConnectedUrl())
	})
}

func (mq *natsMessageQueue) initWaiters() error {
	if mq.conn == nil {
		return nil
	}
	mq.waiterInbox = nats.NewInbox()
	mq.debugf("waiter init inbox=%s", mq.waiterInbox)
	_, err := mq.conn.Subscribe(mq.waiterInbox+".*", func(msg *nats.Msg) {
		mq.onWaiterReply(msg.Subject)
	})
	if err != nil {
		return err
	}
	return nil
}

func (mq *natsMessageQueue) waiterReplySubject(waiterID int64) string {
	return mq.waiterInbox + "." + strconv.FormatInt(waiterID, 10)
}

func (mq *natsMessageQueue) onWaiterReply(subject string) {
	if mq.waiterInbox == "" {
		return
	}
	prefix := mq.waiterInbox + "."
	if !strings.HasPrefix(subject, prefix) {
		return
	}
	idStr := strings.TrimPrefix(subject, prefix)
	waiterID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		mq.debugf("waiter reply parse failed subject=%s err=%v", subject, err)
		return
	}
	if mq.delWaiter(waiterID) {
		mq.debugf("waiter ack received waiter_id=%d", waiterID)
	} else {
		mq.debugf("waiter ack ignored waiter_id=%d reason=missing", waiterID)
	}
}

func (mq *natsMessageQueue) addWaiter(waiterID int64, msg *nats.Msg) {
	mq.waiters.Set(waiterID, msg)
	mq.debugf("waiter add waiter_id=%d subject=%s", waiterID, msg.Subject)
	timeout := mq.cfg.publishAckTimeout
	time.AfterFunc(timeout, func() {
		if !mq.delWaiter(waiterID) {
			return
		}
		mq.connStats.onPublishAckDropped()
		mq.debugf("waiter timeout waiter_id=%d timeout=%s", waiterID, timeout)
	})
}

func (mq *natsMessageQueue) delWaiter(waiterID int64) bool {
	_, ok := mq.waiters.GetAndDelete(waiterID)
	return ok
}

// Close 关闭 NATS 连接。
func (mq *natsMessageQueue) Close() {
	mq.debugf("queue close start")
	mq.conn.Close()
	mq.debugf("queue close done")
}
