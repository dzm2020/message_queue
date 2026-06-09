package queue

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
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

type stressStats struct {
	success      int64
	failed       int64
	timeoutCount int64
	badReply     int64
	disconnects  int64
	reconnects   int64
}

type latencyRecorder struct {
	mu   sync.Mutex
	data []int64
}

func (r *latencyRecorder) Add(d time.Duration) {
	r.mu.Lock()
	r.data = append(r.data, d.Nanoseconds())
	r.mu.Unlock()
}

func (r *latencyRecorder) Percentiles() (time.Duration, time.Duration, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.data) == 0 {
		return 0, 0, 0
	}
	cp := make([]int64, len(r.data))
	copy(cp, r.data)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	p95 := cp[int(float64(len(cp)-1)*0.95)]
	p99 := cp[int(float64(len(cp)-1)*0.99)]
	max := cp[len(cp)-1]
	return time.Duration(p95), time.Duration(p99), time.Duration(max)
}

type secondSample struct {
	requests     int64
	success      int64
	failed       int64
	timeouts     int64
	maxLatencyNs int64
}

type timelineRecorder struct {
	mu       sync.Mutex
	bySecond map[int64]*secondSample
}

func newTimelineRecorder() *timelineRecorder {
	return &timelineRecorder{
		bySecond: make(map[int64]*secondSample),
	}
}

func (r *timelineRecorder) Add(elapsed time.Duration, latency time.Duration, ok bool, timeout bool) {
	sec := int64(elapsed / time.Second)
	if sec < 0 {
		sec = 0
	}

	r.mu.Lock()
	s, exists := r.bySecond[sec]
	if !exists {
		s = &secondSample{}
		r.bySecond[sec] = s
	}
	s.requests++
	if ok {
		s.success++
	} else {
		s.failed++
	}
	if timeout {
		s.timeouts++
	}
	latencyNs := latency.Nanoseconds()
	if latencyNs > s.maxLatencyNs {
		s.maxLatencyNs = latencyNs
	}
	r.mu.Unlock()
}

func (r *timelineRecorder) log(t *testing.T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.bySecond) == 0 {
		return
	}
	seconds := make([]int64, 0, len(r.bySecond))
	for sec := range r.bySecond {
		seconds = append(seconds, sec)
	}
	sort.Slice(seconds, func(i, j int) bool { return seconds[i] < seconds[j] })
	for _, sec := range seconds {
		s := r.bySecond[sec]
		t.Logf("timeline sec=%d req=%d success=%d failed=%d timeout=%d max_latency=%s",
			sec,
			s.requests,
			s.success,
			s.failed,
			s.timeouts,
			time.Duration(s.maxLatencyNs),
		)
	}
}

func TestConcurrentRequestStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skip stress test in short mode")
	}

	url := getenvOrDefault("NATS_URL", nats.DefaultURL)
	total := getenvInt("NATS_STRESS_TOTAL", 10000)
	workers := getenvInt("NATS_STRESS_WORKERS", runtime.NumCPU()*4)
	timeout := time.Duration(getenvInt("NATS_STRESS_TIMEOUT_MS", 2000)) * time.Millisecond
	durationSec := getenvInt("NATS_STRESS_DURATION_SEC", 0)
	subject := fmt.Sprintf("queue.stress.%d", time.Now().UnixNano())
	mode := getenvOrDefault("NATS_STRESS_MODE", "process")

	stats := &stressStats{}
	var opts []nats.Option
	opts = append(opts,
		nats.Timeout(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) {
			atomic.AddInt64(&stats.disconnects, 1)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			atomic.AddInt64(&stats.reconnects, 1)
		}),
	)

	conn, err := nats.Connect(url, opts...)
	if err != nil {
		t.Skipf("skip stress test: cannot connect nats at %s: %v", url, err)
	}
	mq, err := NewNATSMessageQueueFromConnWithOptions(conn)
	if err != nil {
		t.Fatalf("create queue failed: %v", err)
	}
	defer func() {
		if closer, ok := mq.(interface{ Close() }); ok {
			closer.Close()
		}
	}()

	stopResponder, err := startResponderForStress(t, mode, url, subject, mq)
	if err != nil {
		t.Fatalf("start responder failed: %v", err)
	}
	defer stopResponder()

	capHint := total
	if durationSec > 0 {
		capHint = workers * durationSec * 100
	}
	latency := &latencyRecorder{data: make([]int64, 0, capHint)}
	timeline := newTimelineRecorder()
	start := time.Now()
	endAt := start.Add(time.Duration(durationSec) * time.Second)

	jobs := make(chan int, workers*2)
	var wg sync.WaitGroup
	var seq uint64

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				var id uint64
				if durationSec > 0 {
					if time.Now().After(endAt) {
						return
					}
					id = atomic.AddUint64(&seq, 1)
				} else {
					v, ok := <-jobs
					if !ok {
						return
					}
					id = uint64(v)
				}

				payload := []byte(fmt.Sprintf("worker=%d id=%d", workerID, id))
				reqStart := time.Now()
				reply, reqErr := mq.Request(subject, payload, timeout)
				reqLatency := time.Since(reqStart)
				latency.Add(reqLatency)
				if reqErr != nil {
					atomic.AddInt64(&stats.failed, 1)
					isTimeout := errors.Is(reqErr, nats.ErrTimeout) || strings.Contains(strings.ToLower(reqErr.Error()), "timeout")
					if isTimeout {
						atomic.AddInt64(&stats.timeoutCount, 1)
					}
					timeline.Add(time.Since(start), reqLatency, false, isTimeout)
					continue
				}
				if string(reply) != string(payload) {
					atomic.AddInt64(&stats.failed, 1)
					atomic.AddInt64(&stats.badReply, 1)
					timeline.Add(time.Since(start), reqLatency, false, false)
					continue
				}
				atomic.AddInt64(&stats.success, 1)
				timeline.Add(time.Since(start), reqLatency, true, false)
			}
		}(i)
	}

	if durationSec > 0 {
		time.Sleep(time.Until(endAt))
		close(jobs)
	} else {
		for i := 0; i < total; i++ {
			jobs <- i
		}
		close(jobs)
	}
	wg.Wait()

	elapsed := time.Since(start)
	p95, p99, maxLatency := latency.Percentiles()
	success := atomic.LoadInt64(&stats.success)
	failed := atomic.LoadInt64(&stats.failed)
	timeoutCount := atomic.LoadInt64(&stats.timeoutCount)
	badReply := atomic.LoadInt64(&stats.badReply)
	reconnects := atomic.LoadInt64(&stats.reconnects)
	disconnects := atomic.LoadInt64(&stats.disconnects)
	totalRequests := success + failed
	timeoutRate := safeDivide(timeoutCount, totalRequests)
	errorRate := safeDivide(failed, totalRequests)

	timeline.log(t)

	t.Logf("stress done mode=%s total=%d duration_sec=%d requests=%d success=%d failed=%d workers=%d elapsed=%s qps=%.2f p95=%s p99=%s max=%s timeout_rate=%.4f error_rate=%.4f bad_reply=%d disconnects=%d reconnects=%d",
		mode,
		total,
		durationSec,
		totalRequests,
		success,
		failed,
		workers,
		elapsed,
		safeQPS(success, elapsed),
		p95,
		p99,
		maxLatency,
		timeoutRate,
		errorRate,
		badReply,
		disconnects,
		reconnects,
	)

	if failed > 0 {
		t.Fatalf("stress failed: failed=%d success=%d", failed, success)
	}
}

func TestStressResponderProcess(t *testing.T) {
	if os.Getenv("QUEUE_STRESS_HELPER_PROCESS") != "1" {
		t.Skip("helper process entry")
	}
	subject := os.Getenv("QUEUE_STRESS_SUBJECT")
	if subject == "" {
		t.Fatal("QUEUE_STRESS_SUBJECT is empty")
	}
	url := getenvOrDefault("NATS_URL", nats.DefaultURL)
	conn, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("helper connect failed: %v", err)
	}
	mq, err := NewNATSMessageQueueFromConnWithOptions(conn)
	if err != nil {
		t.Fatalf("create queue failed: %v", err)
	}
	defer func() {
		if closer, ok := mq.(interface{ Close() }); ok {
			closer.Close()
		}
	}()
	sub, err := mq.Subscribe(subject, &echoSubscriber{})
	if err != nil {
		t.Fatalf("helper subscribe failed: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	// Keep helper responder alive until parent process kills it.
	for {
		time.Sleep(5 * time.Second)
	}
}

func BenchmarkRequestParallel(b *testing.B) {
	url := getenvOrDefault("NATS_URL", nats.DefaultURL)
	subject := fmt.Sprintf("queue.bench.%d", time.Now().UnixNano())
	timeout := time.Duration(getenvInt("NATS_BENCH_TIMEOUT_MS", 2000)) * time.Millisecond

	conn, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		b.Skipf("skip benchmark: cannot connect nats at %s: %v", url, err)
	}
	mq, err := NewNATSMessageQueueFromConnWithOptions(conn)
	if err != nil {
		b.Fatalf("create queue failed: %v", err)
	}
	defer func() {
		if closer, ok := mq.(interface{ Close() }); ok {
			closer.Close()
		}
	}()

	sub, err := mq.Subscribe(subject, &echoSubscriber{})
	if err != nil {
		b.Fatalf("subscribe failed: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	b.ReportAllocs()
	b.ResetTimer()

	var seq uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&seq, 1)
			payload := []byte(fmt.Sprintf("bench-%d", id))
			reply, reqErr := mq.Request(subject, payload, timeout)
			if reqErr != nil {
				b.Fatalf("request failed: %v", reqErr)
			}
			if string(reply) != string(payload) {
				b.Fatalf("unexpected reply: got=%q want=%q", reply, payload)
			}
		}
	})
}

func getenvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getenvInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultVal
	}
	return n
}

func startResponderForStress(t *testing.T, mode, url, subject string, mq IMessageQue) (func(), error) {
	if mode == "local" {
		sub, err := mq.Subscribe(subject, &echoSubscriber{})
		if err != nil {
			return nil, err
		}
		return func() { _ = sub.Unsubscribe() }, nil
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestStressResponderProcess$", "-test.v")
	cmd.Env = append(
		os.Environ(),
		"QUEUE_STRESS_HELPER_PROCESS=1",
		"QUEUE_STRESS_SUBJECT="+subject,
		"NATS_URL="+url,
	)
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	readyDeadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(readyDeadline) {
		payload := []byte("probe")
		reply, err := mq.Request(subject, payload, 200*time.Millisecond)
		if err == nil && string(reply) == string(payload) {
			return func() {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}, nil
		}
		lastErr = err
		time.Sleep(80 * time.Millisecond)
	}

	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	return nil, fmt.Errorf("helper responder not ready: %w", lastErr)
}

func safeDivide(num, den int64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

func safeQPS(success int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(success) / elapsed.Seconds()
}
