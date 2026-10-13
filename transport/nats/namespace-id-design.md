# NATS Namespace and Service ID Design Notes

## Subject 组成

`natsrpc` 的服务 subject 由以下部分组成：

```text
namespace.service.id
```

空的段会被跳过。因此没有 namespace 或 service ID 时，subject 会退化为：

```text
service
namespace.service
service.id
```

## Server/register 侧的当前实现

Kratos NATS wrapper 当前把两个配置放在不同层级：

- `Namespace(ns)` 是 `ServerOption`，保存在 `Server` 上。同一个 `Server` 注册的所有 service 共用这个 namespace。
- service ID 使用底层的 `natsrpc.WithServiceID(id)`，属于单次 service 注册的 `ServiceOption`。
- `Server.Register` 通过 `natsrpc.ServiceIDFromOptions` 解析注册选项。解析结果用于 wrapper 的 server-side endpoint；
  原始 `ServiceOption` 会继续传给底层 `natsrpc.Server`，由底层再次解析并决定实际 subject。

典型用法：

```go
srv := nats.NewServer(
    nats.Address("nats://localhost:4222"),
    nats.Namespace("example"),
)

api.RegisterGreeterNRServer(
    srv,
    greeterSvc,
    natsrpc.WithServiceID("greeter-1"),
)

api.RegisterUserNRServer(
    srv,
    userSvc,
    natsrpc.WithServiceID("user-1"),
)
```

在这个例子中，RPC 请求使用的真实订阅 subject 和 server-side `Transport.Endpoint()` 分别是：

```text
example.Greeter.greeter-1
example.User.user-1
```

如果不传 `natsrpc.WithServiceID`，service ID 为空，subject 和 server-side transport endpoint 退化为 `namespace.service`。

注册时 wrapper 不会额外生成或注入 `natsrpc.WithServiceID`：它只解析当前 options 得到 service ID，
并用这个值构造 server-side `Transport.Endpoint()`；随后把原始 options 交给底层 `natsrpc.Server.Register`。
因此正常使用 `natsrpc.WithServiceID` 时，两层得到的是同一个有效 ID（重复设置时以最后一个 `WithServiceID` 为准）。

wrapper 总会在传给底层的 options 末尾追加 `natsrpc.WithServiceNamespace(s.namespace)`，所以 Server 级 namespace（为空时
表示默认 namespace）会覆盖同一组 options 中的 service-level `WithServiceNamespace`。wrapper 构造 endpoint 时也只读取
`nats.Namespace`；不要用 service-level `WithServiceNamespace` 为同一个 Server 注册不同 namespace。需要不同 namespace
时应创建不同的 Kratos NATS Server。

`Register` 在 `Start` 之前只缓存服务描述符，真正的底层订阅在 `Start` 中创建；`Start` 之后再注册则立即订阅。

## Server.Endpoint() 的含义

`Server.Endpoint()` 实现的是 `transport.Endpointer`，返回的是用于服务注册的 **NATS broker 地址**，不是某个
service 的 subject。默认情况下，namespace 会作为 URL query 附加到 broker 地址：

```text
nats://127.0.0.1:4222?namespace=example
```

如果通过 `Endpoint(*url.URL)` 显式设置了 endpoint，则直接返回显式设置的 URL，不再根据 `Address` 或 `Namespace`
生成 query。如果通过 `Connection(*nats.Conn)` 传入已连接的连接，且没有显式设置 `Address` 或 `Endpoint`，则使用
连接当前 broker 的地址。这个 endpoint 与请求上下文中的 `Transport.Endpoint()` 含义不同：前者指向 broker，后者
表示当前 service 的 subject 路径。

## Client 侧的当前实现

客户端当前使用：

- `WithNamespace(namespace)`：`ClientOption`，决定客户端请求使用的 namespace。
- `natsrpc.WithCallID(id)`：单次调用的 `CallOption`，决定请求访问的 service 实例 ID。

示例：

```go
client, _ := nats.Dial(ctx, nats.WithNamespace("example"))
greeter := api.NewGreeterNRClient(client)

reply, err := greeter.SayHello(ctx, req, natsrpc.WithCallID("greeter-1"))
```

客户端 `Transport.Endpoint()` 当前只表达：

```text
namespace.service
```

它不包含 `WithCallID` 传入的 ID。`natsrpc.CallOption` 对 wrapper 是 opaque 的，wrapper 不解析其中的 ID；
实际请求仍由底层 `natsrpc` 根据 `WithCallID` 路由到对应的 subject。于是，在上面的例子中，客户端 middleware
看到的是 `example.Greeter`，服务端 middleware 看到的是 `example.Greeter.greeter-1`。

## 当前限制和后续方向

当前实现的 namespace 是 Server 级配置，因此同一个 Server 无法为不同 service 使用不同 namespace。若需要不同
namespace，需要使用不同的 Kratos NATS Server。

客户端 transport endpoint 也无法表达 call-level service ID。当前应继续使用 `natsrpc.WithCallID`，不要在 wrapper
内维护 `map[serviceName]serviceID`，因为同一个 service 可能需要访问多个实例，且调用级 ID 与 client 级状态容易失配。

Kratos wrapper 目前也继续复用一个底层 `natsrpc.Server`，没有为每个 service 创建独立的底层 server。
