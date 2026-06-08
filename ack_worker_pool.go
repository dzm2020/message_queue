package queue

import (
	"hash/fnv"
	"log"
	"sync"
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
	stats   *connectionEventStats

	workerChs []chan ackTask
	mu        sync.RWMutex
	closed    bool
	once      sync.Once
	wg        sync.WaitGroup
}

func newAckWorkerPool(conn *nats.Conn, workerCount int, queueSize int, timeout time.Duration, stats *connectionEventStats) *ackWorkerPool {
	if workerCount <= 0 {
		workerCount = 1
	}
	if queueSize <= 0 {
		queueSize = 1
	}

	pool := &ackWorkerPool{
		conn:      conn,
		timeout:   timeout,
		stats:     stats,
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
		if p.stats != nil {
			p.stats.onPublishAckDropped()
		}
		return ErrNilConnection
	}
	workerCh := p.workerChs[p.workerIndex(subject)]

	select {
	case workerCh <- task:
		return nil
	default:
		if p.stats != nil {
			p.stats.onPublishAckDropped()
		}
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
			var total uint64
			if p.stats != nil {
				total = p.stats.onPublishAckFailure()
			}
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
