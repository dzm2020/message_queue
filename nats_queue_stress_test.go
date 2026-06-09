package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type echoSubscriber struct{}

func (s *echoSubscriber) OnMessage(request []byte, isSync bool, response func(data []byte) error) {
	if !isSync {
		return
	}
	_ = response(request)
}

type countingSubscriber struct {
	syncCount  atomic.Int64
	asyncCount atomic.Int64
}

func (s *countingSubscriber) OnMessage(request []byte, isSync bool, response func(data []byte) error) {
	if isSync {
		s.syncCount.Add(1)
		_ = response(request)
		return
	}
	s.asyncCount.Add(1)
}

func newQueueForPerfTest(tb testing.TB) (IMessageQue, *countingSubscriber, string, func()) {
	tb.Helper()
	conn, err := nats.Connect(nats.DefaultURL, nats.Timeout(2*time.Second))
	if err != nil {
		tb.Skipf("skip perf test: cannot connect nats: %v", err)
	}
	mq, err := NewNATSMessageQueueFromConnWithOptions(conn)
	if err != nil {
		tb.Fatalf("create queue failed: %v", err)
	}
	subject := fmt.Sprintf("queue.perf.%d", time.Now().UnixNano())
	sub := &countingSubscriber{}
	subscription, err := mq.Subscribe(subject, sub)
	if err != nil {
		tb.Fatalf("subscribe failed: %v", err)
	}
	cleanup := func() {
		_ = subscription.Unsubscribe()
		mq.Close()
	}
	return mq, sub, subject, cleanup
}

func TestRequestStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skip stress test in short mode")
	}
	mq, _, subject, cleanup := newQueueForPerfTest(t)
	defer cleanup()

	total := 5000
	workers := 32
	timeout := 1500 * time.Millisecond

	jobs := make(chan int, workers*2)
	var okCount atomic.Int64
	var errCount atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range jobs {
				payload := []byte(fmt.Sprintf("req-worker=%d id=%d", workerID, id))
				reply, err := mq.Request(subject, payload, timeout)
				if err != nil {
					errCount.Add(1)
					continue
				}
				if string(reply) != string(payload) {
					errCount.Add(1)
					continue
				}
				okCount.Add(1)
			}
		}(i)
	}
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	t.Logf("request stress done total=%d success=%d failed=%d elapsed=%s qps=%.2f",
		total, okCount.Load(), errCount.Load(), elapsed, float64(okCount.Load())/elapsed.Seconds())
	if errCount.Load() > 0 {
		t.Fatalf("request stress failed=%d", errCount.Load())
	}
}

func TestPublishStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skip stress test in short mode")
	}
	mq, sub, subject, cleanup := newQueueForPerfTest(t)
	defer cleanup()

	total := 5000
	workers := 32
	jobs := make(chan int, workers*2)
	var errCount atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range jobs {
				payload := []byte(fmt.Sprintf("pub-worker=%d id=%d", workerID, id))
				if err := mq.Publish(subject, payload); err != nil {
					errCount.Add(1)
				}
			}
		}(i)
	}
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for sub.asyncCount.Load() < int64(total) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	elapsed := time.Since(start)
	t.Logf("publish stress done total=%d received_async=%d failed=%d elapsed=%s qps=%.2f",
		total, sub.asyncCount.Load(), errCount.Load(), elapsed, float64(total)/elapsed.Seconds())
	if errCount.Load() > 0 {
		t.Fatalf("publish stress failed=%d", errCount.Load())
	}
	if sub.asyncCount.Load() < int64(total) {
		t.Fatalf("publish received too few messages got=%d want=%d", sub.asyncCount.Load(), total)
	}
}

func TestMixedRequestPublishStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skip stress test in short mode")
	}
	mq, sub, subject, cleanup := newQueueForPerfTest(t)
	defer cleanup()

	total := 4000
	workers := 32
	jobs := make(chan int, workers*2)
	var reqOK atomic.Int64
	var pubOK atomic.Int64
	var errCount atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range jobs {
				payload := []byte(fmt.Sprintf("mix-worker=%d id=%d", workerID, id))
				if id%2 == 0 {
					reply, err := mq.Request(subject, payload, time.Second)
					if err != nil || string(reply) != string(payload) {
						errCount.Add(1)
						continue
					}
					reqOK.Add(1)
					continue
				}
				if err := mq.Publish(subject, payload); err != nil {
					errCount.Add(1)
					continue
				}
				pubOK.Add(1)
			}
		}(i)
	}
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	deadline := time.Now().Add(5 * time.Second)
	wantAsync := int64(total / 2)
	for sub.asyncCount.Load() < wantAsync && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if errCount.Load() > 0 {
		t.Fatalf("mixed stress failed=%d", errCount.Load())
	}
	if sub.asyncCount.Load() < wantAsync {
		t.Fatalf("mixed async receive too few got=%d want>=%d", sub.asyncCount.Load(), wantAsync)
	}
}

func BenchmarkRequestParallel(b *testing.B) {
	mq, _, subject, cleanup := newQueueForPerfTest(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	var seq uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&seq, 1)
			payload := []byte(fmt.Sprintf("bench-req-%d", id))
			reply, err := mq.Request(subject, payload, time.Second)
			if err != nil || string(reply) != string(payload) {
				b.Fatalf("request failed err=%v", err)
			}
		}
	})
}

func BenchmarkPublishParallel(b *testing.B) {
	mq, _, subject, cleanup := newQueueForPerfTest(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	var seq uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&seq, 1)
			payload := []byte(fmt.Sprintf("bench-pub-%d", id))
			if err := mq.Publish(subject, payload); err != nil {
				b.Fatalf("publish failed err=%v", err)
			}
		}
	})
}

func BenchmarkMixedParallel(b *testing.B) {
	mq, _, subject, cleanup := newQueueForPerfTest(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	var seq uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&seq, 1)
			payload := []byte(fmt.Sprintf("bench-mix-%d", id))
			if id%2 == 0 {
				reply, err := mq.Request(subject, payload, time.Second)
				if err != nil || string(reply) != string(payload) {
					b.Fatalf("mixed request failed err=%v", err)
				}
				continue
			}
			if err := mq.Publish(subject, payload); err != nil {
				b.Fatalf("mixed publish failed err=%v", err)
			}
		}
	})
}
