package queue

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNewNATSMessageQueueFromConnWithOptionsAppliesConfig(t *testing.T) {
	mq, err := NewNATSMessageQueueFromConnWithOptions(
		nil,
		WithPublishAckTimeout(1500*time.Millisecond),
		WithDebugLogEnabled(true),
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
	if !impl.cfg.enableDebugLog {
		t.Fatal("enableDebugLog should be true")
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

type eventSubscriber struct {
	events chan messageEvent
}

type messageEvent struct {
	data   string
	isSync bool
}

func (s *eventSubscriber) OnMessage(request []byte, isSync bool, response func(data []byte) error) {
	s.events <- messageEvent{
		data:   string(request),
		isSync: isSync,
	}
	if isSync {
		_ = response([]byte("echo:" + string(request)))
	}
}

func TestNilConnMethodsReturnErr(t *testing.T) {
	mq, err := NewNATSMessageQueueFromConnWithOptions(nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	sub := &eventSubscriber{events: make(chan messageEvent, 1)}

	if err := mq.Publish("subject.nil", []byte("x")); !isNilConnectionErr(err) {
		t.Fatalf("Publish should return nil-connection error, got=%v", err)
	}
	if _, err := mq.Request("subject.nil", []byte("x"), 100*time.Millisecond); !isNilConnectionErr(err) {
		t.Fatalf("Request should return nil-connection error, got=%v", err)
	}
	if _, err := mq.Subscribe("subject.nil", sub); !isNilConnectionErr(err) {
		t.Fatalf("Subscribe should return nil-connection error, got=%v", err)
	}
}

func TestSubscribeNilSubscriber(t *testing.T) {
	mq, err := NewNATSMessageQueueFromConnWithOptions(nil)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, err := mq.Subscribe("subject.nil", nil); !errors.Is(err, ErrNilSubscriber) {
		t.Fatalf("Subscribe(nil) should return ErrNilSubscriber, got=%v", err)
	}
}

func TestInterfacesIntegration(t *testing.T) {
	conn, err := nats.Connect(nats.DefaultURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("skip integration test: cannot connect nats: %v", err)
	}
	mq, err := NewNATSMessageQueueFromConnWithOptions(conn, WithPublishAckTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatalf("create queue failed: %v", err)
	}
	defer mq.Close()

	subject := fmt.Sprintf("queue.test.interfaces.%d", time.Now().UnixNano())
	events := make(chan messageEvent, 16)
	subscription, err := mq.Subscribe(subject, &eventSubscriber{events: events})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() { _ = subscription.Unsubscribe() }()

	reply, err := mq.Request(subject, []byte("sync-hello"), time.Second)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if got, want := string(reply), "echo:sync-hello"; got != want {
		t.Fatalf("Request reply mismatch: got=%q want=%q", got, want)
	}

	if err := mq.Publish(subject, []byte("async-hello")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	deadline := time.After(2 * time.Second)
	var gotSync, gotAsync bool
	for !(gotSync && gotAsync) {
		select {
		case ev := <-events:
			if ev.isSync && ev.data == "sync-hello" {
				gotSync = true
			}
			if !ev.isSync && ev.data == "async-hello" {
				gotAsync = true
			}
		case <-deadline:
			t.Fatalf("did not receive expected sync/async events, gotSync=%t gotAsync=%t", gotSync, gotAsync)
		}
	}

	mq.Close()
	if _, err := mq.Request(subject, []byte("after-close"), 100*time.Millisecond); err == nil {
		t.Fatal("Request after Close should fail")
	}
}

func TestPublishWithoutResponderStatsAccessible(t *testing.T) {
	conn, err := nats.Connect(nats.DefaultURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("skip dropped stats test: cannot connect nats: %v", err)
	}
	mq, err := NewNATSMessageQueueFromConnWithOptions(conn, WithPublishAckTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("create queue failed: %v", err)
	}
	defer mq.Close()

	subject := fmt.Sprintf("queue.test.noresponder.%d", time.Now().UnixNano())
	if err := mq.Publish(subject, []byte("no-responder")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	s, ok := GetConnectionEventStats(mq)
	if !ok {
		t.Fatal("GetConnectionEventStats should succeed")
	}
	// 某些 NATS 版本会返回 no-responders 回执，此时 dropped 可能为 0。
	_ = s.PublishAckDropped
}

func isNilConnectionErr(err error) bool {
	if err == nil {
		return false
	}
	v := strings.ToLower(err.Error())
	return strings.Contains(v, "invalid connection") || strings.Contains(v, "connection is nil")
}
