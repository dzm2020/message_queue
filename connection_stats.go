package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionEventStats 是连接事件统计快照。
type ConnectionEventStats struct {
	Disconnects       uint64
	Reconnects        uint64
	PublishAckDropped uint64
	DispatcherPanics  uint64
	LastDisconnectErr string
	LastDisconnectAt  time.Time
	LastReconnectAt   time.Time
}

type connectionEventStats struct {
	disconnects      atomic.Uint64
	reconnects       atomic.Uint64
	publishAckDrops  atomic.Uint64
	dispatcherPanics atomic.Uint64
	logger           Logger

	mu                sync.RWMutex
	lastDisconnectErr string
	lastDisconnectAt  time.Time
	lastReconnectAt   time.Time
}

func (s *connectionEventStats) onDisconnect(err error) {
	s.disconnects.Add(1)
	s.mu.Lock()
	if err != nil {
		s.lastDisconnectErr = err.Error()
	} else {
		s.lastDisconnectErr = ""
	}
	s.lastDisconnectAt = time.Now()
	s.mu.Unlock()
	if s.logger != nil {
		if err != nil {
			s.logger.Warnf("queue nats disconnected err=%v", err)
		} else {
			s.logger.Warnf("queue nats disconnected")
		}
	}
}

func (s *connectionEventStats) onReconnect() {
	s.reconnects.Add(1)
	s.mu.Lock()
	s.lastReconnectAt = time.Now()
	s.mu.Unlock()
	if s.logger != nil {
		s.logger.Infof("queue nats reconnected")
	}
}

func (s *connectionEventStats) onPublishAckDropped() uint64 {
	return s.publishAckDrops.Add(1)
}

func (s *connectionEventStats) onDispatcherPanic() uint64 {
	return s.dispatcherPanics.Add(1)
}

func (s *connectionEventStats) snapshot() ConnectionEventStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return ConnectionEventStats{
		Disconnects:       s.disconnects.Load(),
		Reconnects:        s.reconnects.Load(),
		PublishAckDropped: s.publishAckDrops.Load(),
		DispatcherPanics:  s.dispatcherPanics.Load(),
		LastDisconnectErr: s.lastDisconnectErr,
		LastDisconnectAt:  s.lastDisconnectAt,
		LastReconnectAt:   s.lastReconnectAt,
	}
}

// GetConnectionEventStats 返回连接事件统计快照。
func GetConnectionEventStats(mq IMessageQue) (ConnectionEventStats, bool) {
	impl, ok := mq.(*natsMessageQueue)
	if !ok {
		return ConnectionEventStats{}, false
	}
	return impl.ConnectionEventStats(), true
}
