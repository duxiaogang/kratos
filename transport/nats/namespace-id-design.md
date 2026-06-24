# NATS Namespace and Service ID Design Notes

## 背景

`natsrpc` 的 subject 由以下部分组成：

```text
namespace.service.id
```

在 `natsrpc` 的模型里，`namespace` 和 `id` 都属于 service 注册选项：

- `natsrpc.WithServiceNamespace(namespace)`
- `natsrpc.WithServiceID(id)`

也就是说，它们是注册某个 service 时的属性，而不是底层 `natsrpc.Server` 的属性。

## 当前问题

Kratos NATS wrapper 里曾把 `id` 设计成 `Server` 级字段。这样同一个 `Server` 下注册的多个 service 会共享同一个 id：

```text
namespace.Greeter.instance-1
namespace.User.instance-1
```

这不适合每个 service 需要独立 id 的场景。

把 `id` 移到 service 注册级别后，又引出了 `namespace` 的归属问题。严格按 `natsrpc` 的模型看，`namespace` 也应该和 `id` 一样属于注册级配置。

## 讨论结论

server/register 侧应该区分两类配置：

- `ServerOption`: NATS broker 连接、生命周期、中间件、超时、编码器等服务端运行时配置。
- `RegistrarOption`: 本次 service 注册的 subject 相关配置，包括 `namespace` 和 `serviceID`。

理想用法：

```go
srv := nats.NewServer(
    nats.Address("nats://localhost:4222"),
)

api.RegisterGreeterNRServer(
    nats.NewRegistrar(
        srv,
        nats.Namespace("example"),
        nats.ServiceID("greeter-1"),
    ),
    svc,
)
```

这样 `Greeter` 的真实订阅 subject 和 server-side `Transport.Endpoint()` 都应为：

```text
example.Greeter.greeter-1
```

## Client 侧取舍

client 侧的问题和 server/register 侧不同。

当前 `natsrpc` 支持：

- `natsrpc.WithClientNamespace(namespace)`: client 级 namespace。
- `natsrpc.WithCallID(id)`: 单次调用级 id。

当前没有 `WithCallNamespace`。这只影响“客户端是否能每次调用动态切 namespace”，不影响 server 侧 namespace 应该归属于 service 注册这一判断。

当前建议先保持 client 侧用法：

```go
client, _ := nats.Dial(ctx, nats.WithNamespace("example"))
greeter := api.NewGreeterNRClient(client)

reply, err := greeter.SayHello(ctx, req, natsrpc.WithCallID("greeter-1"))
```

也就是说：

- server/register 侧：`namespace` 和 `id` 都应该进入 `RegistrarOption`。
- client/call 侧：`namespace` 暂时仍是 `ClientOption`，`id` 由 `natsrpc.WithCallID` 每次调用传入。

## Client Endpoint TODO

当前 client-side `Transport.Endpoint()` 暂时只表达：

```text
namespace.service
```

它不包含 `WithCallID` 传入的 id，因为 `natsrpc.CallOption` 对 wrapper 是 opaque 的，wrapper 不应该解析底层 option。

后续如果需要 client middleware 看到完整 endpoint：

```text
namespace.service.id
```

更合理的方向是在代码生成层显式暴露 service id，而不是在 wrapper 里维护 `service -> id` map。

可能方向：

```go
greeter := api.NewGreeterNRClient(client, nats.ServiceID("greeter-1"))
```

或生成一个带目标实例的派生 client。具体 API 以后再设计。

## 不建议的方向

不建议在 Kratos wrapper 里维护：

```go
map[serviceName]serviceID
```

原因：

- 同一个 service 可能需要访问多个 id。
- id 绑定在 `Dial` 上会让调用点不直观。
- 容易让 client-side endpoint、真实请求 subject、调用级 option 三者产生不一致。

也不建议为了规避底层 `natsrpc.ServiceInterface.Close()` 的潜在问题，在 Kratos wrapper 里为每个 service 创建独立底层 `natsrpc.Server`。如果底层行为有问题，应后续修改 `natsrpc`。

## 后续改动方向

后续如果继续收敛 server/register 侧，建议：

1. 将 `Namespace(ns)` 从 `ServerOption` 改为 `RegistrarOption`。
2. 从 `Server` 中删除 `namespace` 字段。
3. 在 `pendingService` 中记录 `namespace` 和 `serviceID`。
4. `doRegister` 使用注册项里的 `namespace/id` 追加 `natsrpc.WithServiceNamespace` 和 `natsrpc.WithServiceID`。
5. server-side `Transport.Endpoint()` 使用注册项里的 `namespace.service.id`。
6. `Server.Endpoint()` 只表达 NATS broker 地址，不再携带 `?namespace=...`。

