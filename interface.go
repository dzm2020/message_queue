package queue

import "time"

// IMessageQue 集群通信接口，定义核心通信能力
type IMessageQue interface {
	// Publish 异步发布消息并立即返回（后台执行 ACK 校验）
	Publish(subject string, data []byte) error
	// Request 发送请求并等待回复（同步 RPC 模式）
	Request(subject string, data []byte, timeout time.Duration) ([]byte, error)
	// Subscribe 订阅主题，接收消息（非阻塞，通过回调处理）
	Subscribe(subject string, subscriber ISubscriber) (ISubscription, error)
	Close()
}

// ISubscription 订阅关系接口，用于取消订阅
type ISubscription interface {
	Unsubscribe() error
}

type ISubscriber interface {
	//
	// OnMessage
	//  @Description:
	//  @param request
	//  @param isSync true: Request 同步消息；false: Publish 异步消息
	//  @param response 同步消息回复函数；异步消息场景下可忽略
	//
	OnMessage(request []byte, isSync bool, response func(data []byte) error)
}
