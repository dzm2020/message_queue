# NATS 集群内部流转图

本文档包含两张图：

1. 发布到订阅（Publish/Subscribe）
2. 请求应答（Request/Reply）

---

## 1) Publish/Subscribe 流转图

```mermaid
flowchart LR
    P[Publisher Client] -->|PUB subject=data| S1[NATS Server A]

    subgraph Cluster["NATS Cluster"]
      S1 --> IG1[Server A Interest Check]
      IG1 -->|本地有订阅| LQ1[Local Deliver Queue A]
      IG1 -->|远端节点有兴趣| R1[Route Forward]

      R1 --> S2[NATS Server B]
      S2 --> IG2[Server B Interest Check]
      IG2 --> LQ2[Local Deliver Queue B]

      R1 --> S3[NATS Server C]
      S3 --> IG3[Server C Interest Check]
      IG3 --> LQ3[Local Deliver Queue C]
    end

    LQ1 --> SUB1[Subscriber A1/A2]
    LQ2 --> SUB2[Subscriber B1/B2]
    LQ3 --> SUB3[Subscriber C1/C2]
```

简要说明：

- 发布消息先到客户端直连节点（如 A）。
- A 先本地匹配，再按路由兴趣转发到“有该主题订阅兴趣”的节点。
- 远端节点再次做本地匹配并投递给本地订阅者。

---

## 2) Request/Reply 流转图

```mermaid
sequenceDiagram
    participant CReq as Requester Client
    participant SA as NATS Server A
    participant SB as NATS Server B
    participant CSub as Responder Client

    CReq->>SA: REQ service.foo (reply=_INBOX.X)
    SA->>SA: 本地 interest check
    SA->>SB: route forward (service.foo, reply=_INBOX.X)
    SB->>SB: 本地 interest check
    SB->>CSub: deliver request(service.foo)

    CSub->>SB: PUB _INBOX.X (response)
    SB->>SB: 查找 _INBOX.X 订阅所在路由
    SB->>SA: route forward (_INBOX.X)
    SA->>CReq: deliver response(_INBOX.X)

    Note over CReq: 等待超时则返回 timeout
```

简要说明：

- Request 本质是“带 reply inbox 的发布”。
- Responder 收到请求后，向 reply inbox 发布响应。
- 集群按 inbox 兴趣把响应路由回最初请求方所在节点，再交付给请求客户端。
- 请求方在超时窗口内未收到响应会返回 timeout。
