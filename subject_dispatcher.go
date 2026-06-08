package queue

import (
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

type subjectDispatcher struct {
	subject    string
	subscriber ISubscriber
	stats      *connectionEventStats

	msgCh      chan *nats.Msg
	stop       chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
	panicCount atomic.Uint64
}

func newSubjectDispatcher(subject string, subscriber ISubscriber, queueSize int, stats *connectionEventStats) *subjectDispatcher {
	if queueSize <= 0 {
		queueSize = defaultSubjectQueueSize
	}

	d := &subjectDispatcher{
		subject:    subject,
		subscriber: subscriber,
		stats:      stats,
		msgCh:      make(chan *nats.Msg, queueSize),
		stop:       make(chan struct{}),
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-d.stop:
				return
			case msg := <-d.msgCh:
				if msg == nil {
					continue
				}
				d.handleSafely(msg)
			}
		}
	}()

	return d
}

func (d *subjectDispatcher) handleSafely(msg *nats.Msg) {
	defer func() {
		if r := recover(); r != nil {
			subjectPanicCount := d.panicCount.Add(1)
			var total uint64
			if d.stats != nil {
				total = d.stats.onDispatcherPanic()
			}
			log.Printf("queue dispatcher recovered panic subject=%s subject_panic_count=%d total_panic_count=%d panic=%v\n%s",
				d.subject, subjectPanicCount, total, r, debug.Stack())
		}
	}()

	d.handleMessage(msg)
}

func (d *subjectDispatcher) handleMessage(msg *nats.Msg) {
	isPublishMessage := msg.Header.Get(publishRequestHeader) == "1"
	isSync := !isPublishMessage

	var once sync.Once
	var responded atomic.Bool
	response := func(data []byte) error {
		if msg.Reply == "" {
			return nil
		}

		var publishErr error
		once.Do(func() {
			responded.Store(true)
			publishErr = msg.Respond(data)
		})
		return publishErr
	}

	request := append([]byte(nil), msg.Data...)
	d.subscriber.OnMessage(request, isSync, response)

	// Publish 异步消息由框架自动 ACK，业务无需手动调用 response。
	if isPublishMessage && !responded.Load() {
		_ = response(nil)
	}
}

func (d *subjectDispatcher) stopAndWait() {
	d.once.Do(func() {
		close(d.stop)
		d.wg.Wait()
	})
}
