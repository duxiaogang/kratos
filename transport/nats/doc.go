// Package nats 实现基于 NATS 的 Kratos transport.Server 和 transport.Client。
//
// NATS broker 提供位置透明和队列组内负载均衡，因此客户端按服务名寻址，
// 不需要像 gRPC 那样配置 selector 或 discovery resolver。namespace 和 service
// id 会组成服务 endpoint，并分别通过 WithNamespace/WithCallID 选择目标。
//
// 请求 header 由 client middleware 写入 Transport.RequestHeader，经 natsrpc
// 传给 server；响应 header 可以由 server middleware 写入 ReplyHeader，并由
// client middleware 在 Request 返回后读取。Publish 没有响应，所以不会有 reply
// header。
package nats
