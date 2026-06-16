// Package nats 实现了基于 NATS 的 Kratos 传输层（transport.Server /
// transport.Client），底层使用 natsrpc。NATS broker 自带位置透明与组内负载
// 均衡，因此该传输层不像 gRPC 那样需要 selector/discovery 解析器。
//
// # 已知限制：不支持 reply header（server → client 响应头透传）
//
// natsrpc v0.7.0 在请求/响应链路上没有为「自定义响应 header」预留通道：
//   - server 端：natsrpc 在内部构造响应消息，只写入 error header
//     （makeErrorHeader），Interceptor 的签名是 (interface{}, error)，
//     没有任何途径往响应消息追加 header。
//   - client 端：natsrpc 的 Request 只回传解码后的 reply，响应消息的
//     Header 被读取 error 后即丢弃，不对外暴露。
//
// 因此 Transport.ReplyHeader() 这个方法虽然存在（transport.Transporter
// 接口要求），但写进去的内容无法送达对端，server 端中间件/handler 往
// ReplyHeader() 写的 header 会被静默丢弃。
//
// 影响范围：Kratos 的全部内置中间件都不受影响。metadata、tracing 等真正
// 依赖 header 的中间件用的是 RequestHeader()（client → server 方向），而
// 该方向在本传输层是打通的（client 经 natsrpc 的 _ns_user header 发出，
// server 经 natsrpc.CallHeader 读取）。唯一受影响的是「用户自定义中间件想
// 用 ReplyHeader() 做 server→client 响应头透传」这种高级用法，对应 gRPC 的
// grpc.SetHeader / HTTP 响应头。
//
// 若将来确需支持，需要给 natsrpc 打 patch（server 端的 handle/Reply 支持携带
// 自定义 header、client 端 call 暴露 reply.Header），在 Kratos 这一层无法兜住。
package nats
