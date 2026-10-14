# NATS transport examples

这些例子使用 `github.com/go-kratos/kratos/v2/transport/nats`，每个目录只演示一个主要功能。目录中的 `main_test.go` 会调用同一个 `run` 函数，因此命令行演示和集成测试不会出现两套行为。

## 准备 NATS

可以使用本地安装的 `nats-server`：

```bash
nats-server
```

也可以使用 Docker：

```bash
docker run --rm -p 4222:4222 nats:latest
```

默认连接 `nats://127.0.0.1:4222`。如果 broker 在其他地址，设置 `NATS_URL`：

```bash
NATS_URL=nats://127.0.0.1:4223 go run ./examples/natsrpc/request
```

## 运行例子

```bash
go run ./examples/natsrpc/request
go run ./examples/natsrpc/publish
go run ./examples/natsrpc/namespace
go run ./examples/natsrpc/service_id
go run ./examples/natsrpc/error_handler
go run ./examples/natsrpc/recovery
go run ./examples/natsrpc/middleware
go run ./examples/natsrpc/reply_later
go run ./examples/natsrpc/encoder
go run ./examples/natsrpc/options
```

完整组合示例仍然可以分两个终端运行：

```bash
go run ./examples/natsrpc/server
go run ./examples/natsrpc/client
```

功能与目录的对应关系如下：

| 目录 | 演示内容 |
| --- | --- |
| `request` | 请求并等待响应 |
| `publish` | 发布消息，不等待响应 |
| `namespace` | server/client 使用相同 namespace，并检查 transport endpoint |
| `service_id` | `WithServiceID` 注册实例，`WithCallID` 路由到指定实例 |
| `error_handler` | Publish 业务错误由 `ErrorHandler` 上报 |
| `recovery` | publish handler panic 和 `RecoveryHandler`；需要把 panic 转成请求错误时，使用 server middleware recovery |
| `middleware` | client/server middleware、请求 header、响应 header |
| `reply_later` | 返回 `natsrpc.ErrReplyLater`，稍后调用 `natsrpc.Reply` |
| `encoder` | 用 JSON `Encoder` 替换默认 protobuf encoder |
| `options` | `Address`、`Endpoint`、`Timeout`、`Connection`、`NatsOptions`，以及客户端对应 option |

`server` 和 `client` 保留为一个较完整的组合示例，适合第一次接触时直接运行；上面的功能目录适合逐项阅读和修改。

## 运行测试

测试会优先使用 `NATS_URL`。没有设置时，如果 PATH 中有 `nats-server`，每个测试包会启动一个随机端口的临时 broker；没有可用 broker 时测试会跳过：

```bash
go test ./examples/natsrpc/...
go test -race ./examples/natsrpc/...
```

希望 CI 或本地测试必须连接 broker 时设置 `NATS_REQUIRED=1`。使用共享的外部 broker 时，为避免并行测试的 subject 相互影响，可以串行运行：

```bash
NATS_URL=nats://127.0.0.1:4222 go test -p 1 ./examples/natsrpc/...
```

## 重新生成 API

API 文件已经提交在 `api/`，只有修改 `greeter.proto` 时才需要重新生成：

```bash
go install github.com/byebyebruce/natsrpc/cmd/protoc-gen-natsrpc@v0.7.0
cd examples/natsrpc/api
protoc --proto_path=. --proto_path=<path-to-natsrpc-proto> \
  --go_out=paths=source_relative:. \
  --natsrpc_out=paths=source_relative:. greeter.proto
```
