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

	msgCh      chan *nats.Msg
	stop       chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
	panicCount atomic.Uint64
}

var dispatcherPanicTotal atomic.Uint64

// DispatcherPanicTotal 返回 dispatcher goroutine 已恢复的 panic 总数。
func DispatcherPanicTotal() uint64 {
	return dispatcherPanicTotal.Load()
}

func newSubjectDispatcher(subject string, subscriber ISubscriber, queueSize int) *subjectDispatcher {
	if queueSize <= 0 {
		queueSize = defaultSubjectQueueSize
	}

	d := &subjectDispatcher{
		subject:    subject,
		subscriber: subscriber,
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
			total := dispatcherPanicTotal.Add(1)
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
