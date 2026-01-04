package nats

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/byebyebruce/natsrpc"
	"github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/internal/matcher"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

var (
	_ transport.Server     = (*Server)(nil)
	_ transport.Endpointer = (*Server)(nil)
)

// Server is a NATS server wrapper.
type Server struct {
	server          *natsrpc.Server
	conn            *nats.Conn
	endpoint        *url.URL
	address         string
	timeout         time.Duration
	middleware      matcher.Matcher
	namespace       string
	ownConn         bool
	errorHandler    func(interface{})
	recoveryHandler func(interface{})
	natsOpts        []nats.Option
	baseCtx         context.Context
}

// NewServer creates a NATS server by options.
func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		address:    nats.DefaultURL,
		timeout:    5 * time.Second,
		middleware: matcher.New(),
		ownConn:    true,
		errorHandler: func(i interface{}) {
			log.Errorf("[NATS] error: %v", i)
		},
		recoveryHandler: func(i interface{}) {
			log.Errorf("[NATS] panic: %v", i)
		},
	}
	for _, o := range opts {
		o(srv)
	}
	return srv
}

// Use uses a service middleware with selector.
// selector:
//   - '/*'
//   - '/helloworld.v1.Greeter/*'
//   - '/helloworld.v1.Greeter/SayHello'
func (s *Server) Use(selector string, m ...middleware.Middleware) {
	s.middleware.Add(selector, m...)
}

// Endpoint return a real address to registry endpoint.
// examples:
//
//	nats://127.0.0.1:4222?namespace=myapp
func (s *Server) Endpoint() (*url.URL, error) {
	if s.endpoint != nil {
		return s.endpoint, nil
	}
	u, err := url.Parse(s.address)
	if err != nil {
		return nil, err
	}
	u.Scheme = "nats"
	if s.namespace != "" {
		q := u.Query()
		q.Set("namespace", s.namespace)
		u.RawQuery = q.Encode()
	}
	return u, nil
}

// Start starts the NATS server.
func (s *Server) Start(ctx context.Context) error {
	s.baseCtx = ctx

	// Create NATS connection if not provided
	if s.conn == nil {
		conn, err := nats.Connect(s.address, s.natsOpts...)
		if err != nil {
			return fmt.Errorf("[NATS] failed to connect: %w", err)
		}
		s.conn = conn
		s.ownConn = true
	}

	// Create natsrpc server
	serverOpts := []natsrpc.ServerOption{
		natsrpc.WithErrorHandler(s.errorHandler),
		natsrpc.WithServerRecovery(s.recoveryHandler),
	}
	server, err := natsrpc.NewServer(s.conn, serverOpts...)
	if err != nil {
		return fmt.Errorf("[NATS] failed to create server: %w", err)
	}
	s.server = server

	endpoint, _ := s.Endpoint()
	log.Infof("[NATS] server listening on: %s", endpoint.String())

	// Block until context is done
	<-ctx.Done()
	return nil
}

// Stop stops the NATS server.
func (s *Server) Stop(ctx context.Context) error {
	log.Info("[NATS] server stopping")

	if s.server != nil {
		if err := s.server.Close(ctx); err != nil {
			log.Errorf("[NATS] failed to close server: %v", err)
		}
	}

	// Close connection if we own it
	if s.ownConn && s.conn != nil {
		s.conn.Close()
	}

	return nil
}

// Register registers a service with the server.
// This method implements natsrpc.ServiceRegistrar interface.
func (s *Server) Register(sd natsrpc.ServiceDesc, svc any, opts ...natsrpc.ServiceOption) (natsrpc.ServiceInterface, error) {
	// Add namespace option if set
	if s.namespace != "" {
		opts = append([]natsrpc.ServiceOption{natsrpc.WithServiceNamespace(s.namespace)}, opts...)
	}

	// Add timeout option
	opts = append(opts, natsrpc.WithServiceTimeout(s.timeout))

	// Add interceptor for middleware integration
	opts = append(opts, natsrpc.WithServiceInterceptor(s.interceptor(sd.ServiceName)))

	return s.server.Register(sd, svc, opts...)
}

// interceptor creates a natsrpc interceptor that integrates with Kratos middleware.
func (s *Server) interceptor(serviceName string) natsrpc.Interceptor {
	return func(ctx context.Context, method string, req interface{}, invoker natsrpc.Invoker) (interface{}, error) {
		// Build operation name
		operation := fmt.Sprintf("/%s/%s", serviceName, method)

		// Get header from natsrpc context
		header := natsrpc.CallHeader(ctx)
		if header == nil {
			header = make(map[string]string)
		}

		// Build transport
		var ep string
		if s.endpoint != nil {
			ep = s.endpoint.String()
		}
		tr := &Transport{
			endpoint:    ep,
			operation:   operation,
			reqHeader:   headerCarrier(header),
			replyHeader: make(headerCarrier),
		}

		// Inject transport context
		ctx = transport.NewServerContext(ctx, tr)

		// Build handler
		h := func(ctx context.Context, req any) (any, error) {
			return invoker(ctx, req)
		}

		// Apply middleware
		if next := s.middleware.Match(operation); len(next) > 0 {
			h = middleware.Chain(next...)(h)
		}

		return h(ctx, req)
	}
}

// GetServer returns the underlying natsrpc.Server.
// This can be used for advanced configurations.
func (s *Server) GetServer() *natsrpc.Server {
	return s.server
}

// GetConn returns the underlying NATS connection.
func (s *Server) GetConn() *nats.Conn {
	return s.conn
}
