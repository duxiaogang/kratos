package nats

import (
	"net/url"
	"time"

	"github.com/byebyebruce/natsrpc"
	"github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/middleware"
)

// ServerOption 是 NATS 服务端的 option。
type ServerOption func(o *Server)

// Address 设置 NATS 服务端地址。
func Address(addr string) ServerOption {
	return func(s *Server) {
		s.address = addr
	}
}

// Endpoint 设置服务端 endpoint。
func Endpoint(endpoint *url.URL) ServerOption {
	return func(s *Server) {
		s.endpoint = endpoint
	}
}

// Timeout 设置服务端超时时间。
func Timeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.timeout = timeout
	}
}

// Middleware 设置服务端中间件。
func Middleware(m ...middleware.Middleware) ServerOption {
	return func(s *Server) {
		s.middleware.Use(m...)
	}
}

// Connection 使用一个已有的 NATS 连接。
// 设置后，服务端将不再管理该连接的生命周期。
func Connection(conn *nats.Conn) ServerOption {
	return func(s *Server) {
		s.conn = conn
		s.ownConn = false
	}
}

// Namespace 设置服务的 namespace。
func Namespace(ns string) ServerOption {
	return func(s *Server) {
		s.namespace = ns
	}
}

// ServiceID 设置服务实例 id。
// 它会成为 NATS subject（namespace.service.id）以及 transport endpoint 的最后
// 一段，用于在同一 namespace.service 下区分不同的服务实例。留空时 subject 和
// endpoint 退化为 namespace.service。
func ServiceID(id string) ServerOption {
	return func(s *Server) {
		s.id = id
	}
}

// ErrorHandler 设置 error handler。
func ErrorHandler(h func(interface{})) ServerOption {
	return func(s *Server) {
		s.errorHandler = h
	}
}

// RecoveryHandler 设置 recovery handler。
func RecoveryHandler(h func(interface{})) ServerOption {
	return func(s *Server) {
		s.recoveryHandler = h
	}
}

// NatsOptions 设置 NATS 连接的 options。
func NatsOptions(opts ...nats.Option) ServerOption {
	return func(s *Server) {
		s.natsOpts = opts
	}
}

// Encoder 设置自定义的 encoder。
// 默认使用 ProtoEncoder（标准的 google.golang.org/protobuf）。仅当你生成的
// 消息使用了不同的序列化方式时才需要覆盖。
func Encoder(enc natsrpc.Encoder) ServerOption {
	return func(s *Server) {
		s.encoder = enc
	}
}

// ClientOption 是 NATS 客户端的 option。
type ClientOption func(o *clientOptions)

type clientOptions struct {
	address    string
	timeout    time.Duration //请求超时时间
	conn       *nats.Conn
	ownConn    bool
	namespace  string
	natsOpts   []nats.Option
	encoder    natsrpc.Encoder
	middleware []middleware.Middleware
}

// WithAddress 设置客户端连接的 NATS 服务端地址（如 nats://localhost:4222）。
func WithAddress(address string) ClientOption {
	return func(o *clientOptions) {
		o.address = address
	}
}

// WithEndpoint 设置客户端连接的 NATS 服务端地址。
//
// Deprecated: 这里存的是 NATS broker 地址，与 Kratos 的 endpoint（含
// namespace/id 的服务端点）概念不同，名字有误导。请改用 WithAddress。
func WithEndpoint(endpoint string) ClientOption {
	return WithAddress(endpoint)
}

// WithTimeout 设置客户端超时时间。
func WithTimeout(timeout time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.timeout = timeout
	}
}

// WithConnection 使用一个已有的 NATS 连接。
// 设置后，客户端将不再管理该连接的生命周期。
func WithConnection(conn *nats.Conn) ClientOption {
	return func(o *clientOptions) {
		o.conn = conn
		o.ownConn = false
	}
}

// WithNamespace 设置客户端的 namespace。
func WithNamespace(ns string) ClientOption {
	return func(o *clientOptions) {
		o.namespace = ns
	}
}

// WithNatsOptions 设置 NATS 连接的 options。
func WithNatsOptions(opts ...nats.Option) ClientOption {
	return func(o *clientOptions) {
		o.natsOpts = opts
	}
}

// WithEncoder 设置自定义的 encoder。
// 默认使用 ProtoEncoder（标准的 google.golang.org/protobuf）。仅当你生成的
// 消息使用了不同的序列化方式时才需要覆盖。
func WithEncoder(enc natsrpc.Encoder) ClientOption {
	return func(o *clientOptions) {
		o.encoder = enc
	}
}

// WithMiddleware 设置客户端中间件。
func WithMiddleware(m ...middleware.Middleware) ClientOption {
	return func(o *clientOptions) {
		o.middleware = m
	}
}
