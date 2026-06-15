package nats

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/byebyebruce/natsrpc"
	"github.com/nats-io/nats.go"

	ic "github.com/go-kratos/kratos/v2/internal/context"
	"github.com/go-kratos/kratos/v2/internal/matcher"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

var (
	_ transport.Server         = (*Server)(nil)
	_ transport.Endpointer     = (*Server)(nil)
	_ natsrpc.ServiceRegistrar = (*Server)(nil)
)

// pendingService is a Register call deferred until Start.
type pendingService struct {
	sd   natsrpc.ServiceDesc
	svc  any
	opts []natsrpc.ServiceOption
	ref  *serviceRef
}

// Server is a NATS RPC transport server.
//
// The underlying natsrpc.Server subscribes to NATS subjects the moment a
// service is registered. To preserve Kratos lifecycle semantics — a server
// only begins serving when Start runs, after the app is ready and before the
// endpoint is announced to the registry — Register merely buffers descriptors
// and the actual subscription happens in Start.
type Server struct {
	server     *natsrpc.Server
	conn       *nats.Conn
	endpoint   *url.URL
	address    string
	timeout    time.Duration //业务handler timeout
	middleware matcher.Matcher
	namespace  string
	ownConn    bool
	encoder    natsrpc.Encoder

	errorHandler    func(interface{})
	recoveryHandler func(interface{})
	natsOpts        []nats.Option

	baseCtx context.Context

	mu      sync.Mutex
	pending []*pendingService
	started bool
	quit    chan struct{}
	err     error //todo: 这个有什么用？
}

// NewServer creates a NATS RPC server by options.
func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		baseCtx:    context.Background(),
		address:    nats.DefaultURL,
		timeout:    5 * time.Second,
		middleware: matcher.New(),
		encoder:    ProtoEncoder{},
		quit:       make(chan struct{}),
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

// todo: 重写，莫名其妙的代码
// connect establishes the NATS connection and the underlying natsrpc.Server.
// It is idempotent. Caller must hold s.mu.
func (s *Server) connect() error {
	if s.server != nil || s.err != nil {
		return s.err
	}
	if s.conn == nil {
		conn, err := nats.Connect(s.address, s.natsOpts...)
		if err != nil {
			s.err = fmt.Errorf("[NATS] failed to connect: %w", err)
			return s.err
		}
		s.conn = conn
		s.ownConn = true
	}
	server, err := natsrpc.NewServer(s.conn,
		natsrpc.WithErrorHandler(s.errorHandler),
		natsrpc.WithServerRecovery(s.recoveryHandler),
		natsrpc.WithServerEncoder(s.encoder),
	)
	if err != nil {
		s.err = fmt.Errorf("[NATS] failed to create server: %w", err)
		return s.err
	}
	s.server = server
	return nil
}

// Use uses a service middleware with selector.
// selector:
//   - '/*'
//   - '/helloworld.v1.Greeter/*'
//   - '/helloworld.v1.Greeter/SayHello'
func (s *Server) Use(selector string, m ...middleware.Middleware) {
	s.middleware.Add(selector, m...)
}

// Endpoint returns the real address to registry endpoint.
//
//	nats://127.0.0.1:4222?namespace=myapp
func (s *Server) Endpoint() (*url.URL, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buildEndpoint()
}

// buildEndpoint computes and caches the endpoint URL. Caller must hold s.mu.
func (s *Server) buildEndpoint() (*url.URL, error) {
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
	s.endpoint = u
	return u, nil
}

// Register buffers a service descriptor; the actual NATS subscription is
// created when Start runs. It implements natsrpc.ServiceRegistrar so generated
// RegisterXxxNRServer functions can target this server directly.
//
// The returned ServiceInterface delegates to the real service once Start has
// run. Registering after Start subscribes immediately.
func (s *Server) Register(sd natsrpc.ServiceDesc, svc any, opts ...natsrpc.ServiceOption) (natsrpc.ServiceInterface, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ref := &serviceRef{name: sd.ServiceName}
	if s.started {
		real, err := s.doRegister(sd, svc, opts)
		if err != nil {
			return nil, err
		}
		ref.set(real)
		return ref, nil
	}

	s.pending = append(s.pending, &pendingService{sd: sd, svc: svc, opts: opts, ref: ref})
	return ref, nil
}

// doRegister applies Kratos defaults (namespace, timeout, interceptor) then
// registers the service. User-supplied opts are appended last so they win.
// Caller must hold s.mu.
func (s *Server) doRegister(sd natsrpc.ServiceDesc, svc any, opts []natsrpc.ServiceOption) (natsrpc.ServiceInterface, error) {
	merged := make([]natsrpc.ServiceOption, 0, len(opts)+3)
	if s.namespace != "" {
		merged = append(merged, natsrpc.WithServiceNamespace(s.namespace))
	}
	merged = append(merged, natsrpc.WithServiceTimeout(s.timeout))
	merged = append(merged, natsrpc.WithServiceInterceptor(s.interceptor(sd.ServiceName)))
	merged = append(merged, opts...)
	return s.server.Register(sd, svc, merged...)
}

// Start connects, subscribes all registered services, then blocks until Stop.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	s.baseCtx = ctx
	if err := s.connect(); err != nil {
		s.mu.Unlock()
		return err
	}
	for _, p := range s.pending {
		real, err := s.doRegister(p.sd, p.svc, p.opts)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		p.ref.set(real)
	}
	s.pending = nil
	s.started = true
	endpoint, err := s.buildEndpoint()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	quit := s.quit
	s.mu.Unlock()

	log.Infof("[NATS] server listening on: %s", endpoint.String())

	// The app passes a context that is never canceled on shutdown; it stops
	// servers by calling Stop, which closes quit to unblock us here.
	select {
	case <-quit:
	case <-ctx.Done():
	}
	return nil
}

// Stop stops the NATS server and unblocks Start.
func (s *Server) Stop(ctx context.Context) error {
	log.Info("[NATS] server stopping")

	s.mu.Lock()
	select {
	case <-s.quit:
	default:
		close(s.quit)
	}
	server := s.server
	conn := s.conn
	ownConn := s.ownConn
	timeout := s.timeout
	s.started = false
	s.mu.Unlock()

	// natsrpc's Close flushes via FlushWithContext, which requires a deadline.
	// The app may pass a context without one (stopTimeout defaults to 0), so
	// ensure a deadline is present.
	if _, ok := ctx.Deadline(); !ok {
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if server != nil {
		if err := server.Close(ctx); err != nil {
			log.Errorf("[NATS] failed to close server: %v", err)
		}
	}
	if ownConn && conn != nil {
		conn.Close()
	}
	return nil
}

// interceptor bridges a natsrpc handler invocation into the Kratos middleware
// chain, injects the server transport context, and encodes any returned error
// so the full Kratos error model survives the round trip back to the caller.
func (s *Server) interceptor(serviceName string) natsrpc.Interceptor {
	return func(ctx context.Context, method string, req interface{}, invoker natsrpc.Invoker) (interface{}, error) {
		// Merge with the app base context so values (e.g. logger) propagate and
		// app shutdown cancels in-flight handlers.
		ctx, cancel := ic.Merge(ctx, s.baseCtx)
		defer cancel()

		operation := fmt.Sprintf("/%s/%s", serviceName, method)

		reqHeader := natsrpc.CallHeader(ctx)
		if reqHeader == nil {
			reqHeader = make(map[string]string)
		}

		var ep string
		if s.endpoint != nil {
			ep = s.endpoint.String()
		}
		tr := &Transport{
			endpoint:    ep,
			operation:   operation,
			reqHeader:   headerCarrier(reqHeader),
			replyHeader: make(headerCarrier),
		}
		ctx = transport.NewServerContext(ctx, tr)

		h := func(ctx context.Context, req any) (any, error) {
			return invoker(ctx, req)
		}
		if next := s.middleware.Match(operation); len(next) > 0 {
			h = middleware.Chain(next...)(h)
		}

		reply, err := h(ctx, req)
		if err != nil {
			// Hand natsrpc an error whose text is the protojson-encoded Status;
			// it goes into the _ns_error header and the client wrapper decodes
			// it back into a full *errors.Error.
			return nil, errors.New(EncodeError(err))
		}
		//todo: reply header不返回给client？
		return reply, nil
	}
}

// serviceRef is a handle to a registered service. Before Start it is a
// placeholder; afterwards it delegates to the real natsrpc service.
type serviceRef struct {
	mu   sync.Mutex
	name string
	real natsrpc.ServiceInterface
}

func (r *serviceRef) set(real natsrpc.ServiceInterface) {
	r.mu.Lock()
	r.real = real
	r.mu.Unlock()
}

// Name returns the service name.
func (r *serviceRef) Name() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.real != nil {
		return r.real.Name()
	}
	return r.name
}

// Close unsubscribes the service if it is active.
func (r *serviceRef) Close() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.real != nil {
		return r.real.Close()
	}
	return false
}
