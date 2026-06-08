package queue

import (
	"errors"
	"testing"
	"time"
)

type noopSubscriber struct{}

func (s *noopSubscriber) OnMessage(_ []byte, _ bool, _ func(data []byte) error) {}

func TestNewNATSMessageQueueFromConnWithOptionsAppliesConfig(t *testing.T) {
	mq := NewNATSMessageQueueFromConnWithOptions(
		nil,
		WithAckWorkerCount(3),
		WithAckQueueSize(17),
		WithPublishAckTimeout(1500*time.Millisecond),
		WithSubjectQueueSize(33),
	)

	impl, ok := mq.(*natsMessageQueue)
	if !ok {
		t.Fatalf("unexpected queue type %T", mq)
	}

	if impl.cfg.ackWorkerCount != 3 {
		t.Fatalf("ackWorkerCount mismatch: got=%d want=3", impl.cfg.ackWorkerCount)
	}
	if impl.cfg.ackQueueSize != 17 {
		t.Fatalf("ackQueueSize mismatch: got=%d want=17", impl.cfg.ackQueueSize)
	}
	if impl.cfg.publishAckTimeout != 1500*time.Millisecond {
		t.Fatalf("publishAckTimeout mismatch: got=%s want=%s", impl.cfg.publishAckTimeout, 1500*time.Millisecond)
	}
	if impl.cfg.subjectQueueSize != 33 {
		t.Fatalf("subjectQueueSize mismatch: got=%d want=33", impl.cfg.subjectQueueSize)
	}
}

func TestAckWorkerPoolCloseRejectsEnqueue(t *testing.T) {
	pool := newAckWorkerPool(nil, 1, 1, time.Second, nil, nil)
	pool.Close()

	err := pool.Enqueue("subject.a", []byte("payload"))
	if !errors.Is(err, ErrNilConnection) {
		t.Fatalf("enqueue after close should fail with ErrNilConnection, got=%v", err)
	}
}

func TestAckWorkerPoolWorkerIndexStablePerSubject(t *testing.T) {
	pool := newAckWorkerPool(nil, 4, 8, time.Second, nil, nil)
	defer pool.Close()

	a1 := pool.workerIndex("subject.a")
	a2 := pool.workerIndex("subject.a")
	if a1 != a2 {
		t.Fatalf("worker index must be stable for same subject: %d != %d", a1, a2)
	}
}

func TestSubjectDispatcherUsesConfiguredQueueSize(t *testing.T) {
	dispatcher := newSubjectDispatcher("subject.test", &noopSubscriber{}, 25, nil, nil)
	defer dispatcher.stopAndWait()

	if cap(dispatcher.msgCh) != 25 {
		t.Fatalf("dispatcher queue size mismatch: got=%d want=25", cap(dispatcher.msgCh))
	}
}

func TestWithLoggerSetsConfigLogger(t *testing.T) {
	cfg := applyQueueOptions([]QueueOption{
		WithLogger(defaultLogger()),
	})
	if cfg.logger == nil {
		t.Fatal("logger should not be nil")
	}
}

func TestConnectionEventStatsSnapshot(t *testing.T) {
	var stats connectionEventStats
	stats.onDisconnect(errors.New("network down"))
	stats.onReconnect()

	snapshot := stats.snapshot()
	if snapshot.Disconnects != 1 {
		t.Fatalf("disconnect count mismatch: got=%d want=1", snapshot.Disconnects)
	}
	if snapshot.Reconnects != 1 {
		t.Fatalf("reconnect count mismatch: got=%d want=1", snapshot.Reconnects)
	}
	if snapshot.LastDisconnectErr == "" {
		t.Fatal("last disconnect err should not be empty")
	}
	if snapshot.LastDisconnectAt.IsZero() || snapshot.LastReconnectAt.IsZero() {
		t.Fatal("disconnect/reconnect time should be recorded")
	}
}
