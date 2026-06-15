package nats

import (
	"net/url"
	"time"

	"github.com/byebyebruce/natsrpc"
	"github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/middleware"
)

// ServerOption is a NATS server option.
type ServerOption func(o *Server)

// Address with NATS server address.
func Address(addr string) ServerOption {
	return func(s *Server) {
		s.address = addr
	}
}

// Endpoint with server endpoint.
func Endpoint(endpoint *url.URL) ServerOption {
	return func(s *Server) {
		s.endpoint = endpoint
	}
}

// Timeout with server timeout.
func Timeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.timeout = timeout
	}
}

// Middleware with server middleware.
func Middleware(m ...middleware.Middleware) ServerOption {
	return func(s *Server) {
		s.middleware.Use(m...)
	}
}

// Connection with an existing NATS connection.
// If set, the server will not manage the connection lifecycle.
func Connection(conn *nats.Conn) ServerOption {
	return func(s *Server) {
		s.conn = conn
		s.ownConn = false
	}
}

// Namespace with service namespace.
func Namespace(ns string) ServerOption {
	return func(s *Server) {
		s.namespace = ns
	}
}

// ErrorHandler with error handler.
func ErrorHandler(h func(interface{})) ServerOption {
	return func(s *Server) {
		s.errorHandler = h
	}
}

// RecoveryHandler with recovery handler.
func RecoveryHandler(h func(interface{})) ServerOption {
	return func(s *Server) {
		s.recoveryHandler = h
	}
}

// NatsOptions with NATS connection options.
func NatsOptions(opts ...nats.Option) ServerOption {
	return func(s *Server) {
		s.natsOpts = opts
	}
}

// Encoder with custom encoder.
// Defaults to ProtoEncoder (standard google.golang.org/protobuf). Override only
// when your generated messages use a different serialization.
func Encoder(enc natsrpc.Encoder) ServerOption {
	return func(s *Server) {
		s.encoder = enc
	}
}

// ClientOption is a NATS client option.
type ClientOption func(o *clientOptions)

type clientOptions struct {
	endpoint   string
	timeout    time.Duration //request timeout
	conn       *nats.Conn
	ownConn    bool
	namespace  string
	natsOpts   []nats.Option
	encoder    natsrpc.Encoder
	middleware []middleware.Middleware
}

// WithEndpoint with client endpoint.
func WithEndpoint(endpoint string) ClientOption {
	return func(o *clientOptions) {
		o.endpoint = endpoint
	}
}

// WithTimeout with client timeout.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.timeout = timeout
	}
}

// WithConnection with an existing NATS connection.
// If set, the client will not manage the connection lifecycle.
func WithConnection(conn *nats.Conn) ClientOption {
	return func(o *clientOptions) {
		o.conn = conn
		o.ownConn = false
	}
}

// WithNamespace with client namespace.
func WithNamespace(ns string) ClientOption {
	return func(o *clientOptions) {
		o.namespace = ns
	}
}

// WithNatsOptions with NATS connection options.
func WithNatsOptions(opts ...nats.Option) ClientOption {
	return func(o *clientOptions) {
		o.natsOpts = opts
	}
}

// WithEncoder with custom encoder.
// Defaults to ProtoEncoder (standard google.golang.org/protobuf). Override only
// when your generated messages use a different serialization.
func WithEncoder(enc natsrpc.Encoder) ClientOption {
	return func(o *clientOptions) {
		o.encoder = enc
	}
}

// WithMiddleware with client middleware.
func WithMiddleware(m ...middleware.Middleware) ClientOption {
	return func(o *clientOptions) {
		o.middleware = m
	}
}
