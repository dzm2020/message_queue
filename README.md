# message_queue

基于 `github.com/nats-io/nats.go` 封装的轻量消息队列组件，支持：

- 异步发布（`Publish`，后台 ACK 校验）
- 同步请求应答（`Request`，RPC 模式）
- 主题订阅（`Subscribe`，每个 `subject` 独立协程处理）

> 模块路径是 `message_queue`，包名是 `queue`。  
> 建议导入时起别名，例如 `mq "message_queue"`。

---

## 特性

- **发布不阻塞**：`Publish` 立即返回，后台由有界 ACK worker 池做确认。
- **同主题有序**：ACK worker 按 `subject` 哈希固定到同一 worker，保证同主题顺序稳定。
- **订阅隔离**：每个 `subject` 有独立 `dispatcher` 协程，避免跨主题互相阻塞。
- **安全关闭**：`Close()` 会先取消订阅，再停止 dispatcher、ACK 池并关闭连接。
- **基础可观测性**：提供 panic/ACK 失败与丢弃计数函数。

---

## 安装

```bash
go get github.com/nats-io/nats.go
```

---

## 核心接口

```go
type IMessageQue interface {
    Publish(subject string, data []byte) error
    Request(subject string, data []byte, timeout time.Duration) ([]byte, error)
    Subscribe(subject string, subscriber ISubscriber) (ISubscription, error)
    Close()
}
```

`ISubscriber`：

```go
type ISubscriber interface {
    OnMessage(request []byte, isSync bool, response func(data []byte) error)
}
```

- `isSync=true`：来自 `Request` 的同步消息，应按需调用 `response` 回复。
- `isSync=false`：来自 `Publish` 的异步消息，业务可不调用 `response`，框架会自动 ACK。

---

## 快速开始

```go
package main

import (
	"fmt"
	"log"
	"time"

	mq "message_queue"
	"github.com/nats-io/nats.go"
)

type EchoSubscriber struct{}

func (s *EchoSubscriber) OnMessage(request []byte, isSync bool, response func(data []byte) error) {
	if isSync {
		_ = response([]byte("echo:" + string(request)))
	}
}

func main() {
	q, err := mq.NewNATSMessageQueue(
		nats.DefaultURL,
		mq.WithNatsOptions(nats.Timeout(2*time.Second)),
		mq.WithPublishAckTimeout(2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	sub, err := q.Subscribe("demo.echo", &EchoSubscriber{})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 异步发布（立即返回）
	if err := q.Publish("demo.echo", []byte("async hello")); err != nil {
		log.Fatal(err)
	}

	// 同步请求
	reply, err := q.Request("demo.echo", []byte("sync hello"), 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply)) // echo:sync hello
}
```

---

## 配置项（QueueOption）

- `WithNatsOptions(...nats.Option)`：NATS 连接参数
- `WithPublishAckTimeout(time.Duration)`：后台 ACK 超时时间
- `WithLogger(Logger)`：注入日志实现
- `WithDebugLogEnabled(bool)`：是否启用 Debug 日志

---

## 运行测试与压测

```bash
go test ./...
go test ./... -run TestConcurrentRequestStress -v
go test ./... -bench BenchmarkRequestParallel -benchmem -run ^$
```

可选环境变量：

- `NATS_URL`
- `NATS_STRESS_TOTAL`
- `NATS_STRESS_WORKERS`
- `NATS_STRESS_TIMEOUT_MS`
- `NATS_STRESS_DURATION_SEC`
- `NATS_STRESS_MODE` (`process` / `local`)
- `NATS_BENCH_TIMEOUT_MS`

---

## 可观测指标函数

- `GetConnectionEventStats(mq IMessageQue) (ConnectionEventStats, bool)`

---

## 注意事项

- `Publish` 是“异步返回 + 后台 ACK 校验”，`nil` 仅表示“成功入 ACK 队列”。
