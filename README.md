# message_queue

`message_queue` 是基于 `github.com/nats-io/nats.go` 的轻量封装，提供统一的消息队列接口：

- `Publish`：异步发布（框架自动 ACK）
- `Request`：同步请求-应答（RPC）
- `Subscribe`：主题订阅回调
- `Close`：关闭连接

> 模块路径：`message_queue`  
> 包名：`queue`（建议导入别名 `mq`）

---

## 安装

```bash
go get github.com/nats-io/nats.go
```

---

## 接口定义

```go
type IMessageQue interface {
	Publish(subject string, data []byte) error
	Request(subject string, data []byte, timeout time.Duration) ([]byte, error)
	Subscribe(subject string, subscriber ISubscriber) (ISubscription, error)
	Close()
}
```

```go
type ISubscriber interface {
	OnMessage(request []byte, isSync bool, response func(data []byte) error)
}
```

语义说明：

- `isSync=true`：来自 `Request`，业务一般需要调用 `response(...)` 回复。
- `isSync=false`：来自 `Publish`，框架会优先自动 ACK（按当前实现约束，业务不应修改 ACK 内容）。

---

## 构造函数

```go
func NewNATSMessageQueue(url string, queueOptions ...QueueOption) (IMessageQue, error)
func NewNATSMessageQueueFromConn(conn *nats.Conn) (IMessageQue, error)
func NewNATSMessageQueueFromConnWithOptions(conn *nats.Conn, queueOptions ...QueueOption) (IMessageQue, error)
```

---

## 配置项（QueueOption）

- `WithNatsOptions(...nats.Option)`：NATS 连接参数
- `WithPublishAckTimeout(time.Duration)`：Publish ACK 等待超时
- `WithLogger(Logger)`：注入自定义日志器
- `WithDebugLogEnabled(bool)`：启用/关闭 debug 日志

日志接口：

```go
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}
```

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

	if err := q.Publish("demo.echo", []byte("async hello")); err != nil {
		log.Fatal(err)
	}

	reply, err := q.Request("demo.echo", []byte("sync hello"), 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply)) // echo:sync hello
}
```

---

## 可观测性

可通过以下函数获取连接与内部事件统计：

```go
GetConnectionEventStats(mq IMessageQue) (ConnectionEventStats, bool)
```

当前快照包含：

- 断连次数/重连次数
- Publish ACK 超时丢弃次数
- Dispatcher panic 次数
- 最近一次断连错误与断连/重连时间

---

## 测试与压测

基础测试：

```bash
go test ./...
```

接口覆盖测试（包含 Publish/Request/Subscribe/Close）：

```bash
go test ./... -run TestInterfacesIntegration -v
```

压测（需可连接 NATS）：

```bash
go test ./... -run TestRequestStress -v
go test ./... -run TestPublishStress -v
go test ./... -run TestMixedRequestPublishStress -v
```

Benchmark：

```bash
go test ./... -bench BenchmarkRequestParallel -benchmem -run ^$
go test ./... -bench BenchmarkPublishParallel -benchmem -run ^$
go test ./... -bench BenchmarkMixedParallel -benchmem -run ^$
```

---

## 备注

- 当前实现遵循 NATS 原生订阅语义：同一 subject 可重复订阅（会重复收到消息）。
- `Publish` 返回 `nil` 表示消息已成功发送并进入 ACK 跟踪流程，不代表对端业务处理成功。
