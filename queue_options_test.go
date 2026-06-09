package queue

import (
	"errors"
	"testing"
	"time"
)

func TestNewNATSMessageQueueFromConnWithOptionsAppliesConfig(t *testing.T) {
	mq, err := NewNATSMessageQueueFromConnWithOptions(
		nil,
		WithPublishAckTimeout(1500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewNATSMessageQueueFromConnWithOptions failed: %v", err)
	}

	impl, ok := mq.(*natsMessageQueue)
	if !ok {
		t.Fatalf("unexpected queue type %T", mq)
	}

	if impl.cfg.publishAckTimeout != 1500*time.Millisecond {
		t.Fatalf("publishAckTimeout mismatch: got=%s want=%s", impl.cfg.publishAckTimeout, 1500*time.Millisecond)
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
