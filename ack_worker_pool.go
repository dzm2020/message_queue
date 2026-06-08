package queue

import (
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

type ackTask struct {
	subject string
	data    []byte
}

type ackWorkerPool struct {
	conn    *nats.Conn
	timeout time.Duration

	workerChs []chan ackTask
	mu        sync.RWMutex
	closed    bool
	once      sync.Once
	wg        sync.WaitGroup
}

var publishAsyncAckFailureTotal atomic.Uint64
var publishAsyncAckDroppedTotal atomic.Uint64

// PublishAsyncAckFailureTotal 返回 Publish 异步 ACK 失败总数。
func PublishAsyncAckFailureTotal() uint64 {
	return publishAsyncAckFailureTotal.Load()
}

// PublishAsyncAckDroppedTotal 返回 Publish 异步 ACK 入队失败总数。
func PublishAsyncAckDroppedTotal() uint64 {
	return publishAsyncAckDroppedTotal.Load()
}

func newAckWorkerPool(conn *nats.Conn, workerCount int, queueSize int, timeout time.Duration) *ackWorkerPool {
	if workerCount <= 0 {
		workerCount = 1
	}
	if queueSize <= 0 {
		queueSize = 1
	}

	pool := &ackWorkerPool{
		conn:      conn,
		timeout:   timeout,
		workerChs: make([]chan ackTask, workerCount),
	}

	for i := 0; i < workerCount; i++ {
		pool.workerChs[i] = make(chan ackTask, queueSize)
		pool.wg.Add(1)
		go pool.runWorker(i, pool.workerChs[i])
	}

	return pool
}

func (p *ackWorkerPool) Enqueue(subject string, data []byte) error {
	task := ackTask{
		subject: subject,
		data:    append([]byte(nil), data...),
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		publishAsyncAckDroppedTotal.Add(1)
		return ErrNilConnection
	}
	workerCh := p.workerChs[p.workerIndex(subject)]

	select {
	case workerCh <- task:
		return nil
	default:
		publishAsyncAckDroppedTotal.Add(1)
		return ErrPublishAckQueueFull
	}
}

func (p *ackWorkerPool) runWorker(workerID int, ch <-chan ackTask) {
	defer p.wg.Done()

	for task := range ch {
		msg := nats.NewMsg(task.subject)
		msg.Data = task.data
		msg.Header.Set(publishRequestHeader, "1")

		if _, err := p.conn.RequestMsg(msg, p.timeout); err != nil {
			total := publishAsyncAckFailureTotal.Add(1)
			log.Printf("queue publish async ack failed worker=%d subject=%s total_failures=%d err=%v", workerID, task.subject, total, err)
		}
	}
}

func (p *ackWorkerPool) workerIndex(subject string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(subject))
	return int(h.Sum32() % uint32(len(p.workerChs)))
}

func (p *ackWorkerPool) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		for _, ch := range p.workerChs {
			close(ch)
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
}
