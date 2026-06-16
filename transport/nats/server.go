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

// pendingService 表示一次被延迟到 Start 时才真正执行的 Register 调用。
type pendingService struct {
	sd   natsrpc.ServiceDesc
	svc  any
	opts []natsrpc.ServiceOption
	ref  *serviceRef
}

// Server 是 NATS RPC 传输层服务端。
//
// 底层的 natsrpc.Server 会在服务注册的那一刻立即订阅 NATS subject。为了
// 保持 Kratos 的生命周期语义——服务端只在 Start 运行时（即 app 就绪之后、
// endpoint 对外宣告到注册中心之前）才开始提供服务——Register 只负责缓存
// 描述符，真正的订阅发生在 Start 中。
type Server struct {
	server     *natsrpc.Server
	conn       *nats.Conn
	endpoint   *url.URL //fixme: 这个目前没意义
	address    string
	timeout    time.Duration //业务handler timeout
	middleware matcher.Matcher
	namespace  string
	id         string
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
}

// NewServer 通过 options 创建一个 NATS RPC 服务端。
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

// connect 建立 NATS 连接以及底层的 natsrpc.Server。
// 该方法只在 Start 中调用一次；调用方必须持有 s.mu。
func (s *Server) connect() error {
	if s.conn == nil {
		conn, err := nats.Connect(s.address, s.natsOpts...)
		if err != nil {
			return fmt.Errorf("[NATS] failed to connect: %w", err)
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
		return fmt.Errorf("[NATS] failed to create server: %w", err)
	}
	s.server = server
	return nil
}

// Use 注册一个带 selector 的服务中间件。
// selector:
//   - '/*'
//   - '/helloworld.v1.Greeter/*'
//   - '/helloworld.v1.Greeter/SayHello'
func (s *Server) Use(selector string, m ...middleware.Middleware) {
	s.middleware.Add(selector, m...)
}

// Endpoint 返回用于注册中心的真实地址。
//
//	nats://127.0.0.1:4222?namespace=myapp
func (s *Server) Endpoint() (*url.URL, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buildEndpoint()
}

// buildEndpoint 计算并缓存 endpoint URL。调用方必须持有 s.mu。
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

// Register 缓存一个服务描述符；真正的 NATS 订阅会在 Start 运行时创建。
// 它实现了 natsrpc.ServiceRegistrar，因此生成的 RegisterXxxNRServer 函数
// 可以直接以该服务端为目标。
//
// 返回的 ServiceInterface 会在 Start 运行后委托给真实的服务。如果在 Start
// 之后再注册，则会立即订阅。
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

// doRegister 先应用 Kratos 的默认设置（namespace、timeout、interceptor），
// 然后注册服务。用户传入的 opts 追加在最后，因此优先级最高。
// 调用方必须持有 s.mu。
func (s *Server) doRegister(sd natsrpc.ServiceDesc, svc any, opts []natsrpc.ServiceOption) (natsrpc.ServiceInterface, error) {
	merged := make([]natsrpc.ServiceOption, 0, len(opts)+4)
	if s.namespace != "" {
		merged = append(merged, natsrpc.WithServiceNamespace(s.namespace))
	}
	if s.id != "" {
		merged = append(merged, natsrpc.WithServiceID(s.id))
	}
	merged = append(merged, natsrpc.WithServiceTimeout(s.timeout))
	merged = append(merged, natsrpc.WithServiceInterceptor(s.interceptor(sd.ServiceName)))
	merged = append(merged, opts...)
	return s.server.Register(sd, svc, merged...)
}

// Start 建立连接、订阅所有已注册的服务，然后阻塞直到 Stop。
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

	// app 传入的 context 在关闭时永远不会被取消；它通过调用 Stop 来停止
	// 服务端，Stop 会关闭 quit 从而在这里解除阻塞。
	select {
	case <-quit:
	case <-ctx.Done():
	}
	return nil
}

// Stop 停止 NATS 服务端并解除 Start 的阻塞。
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

	// natsrpc 的 Close 通过 FlushWithContext 来 flush，这要求带有 deadline。
	// app 传入的 context 可能没有 deadline（stopTimeout 默认为 0），因此
	// 这里确保一定带有 deadline。
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

// interceptor 把一次 natsrpc handler 调用桥接到 Kratos 中间件链中，注入
// 服务端 transport context，并对返回的 error 进行编码，使完整的 Kratos
// 错误模型能够完整地回传给调用方。
func (s *Server) interceptor(serviceName string) natsrpc.Interceptor {
	return func(ctx context.Context, method string, req interface{}, invoker natsrpc.Invoker) (interface{}, error) {
		// 与 app 的 base context 合并，使其中的值（如 logger）得以传播，
		// 并使 app 关闭时能取消处理中的 handler。
		ctx, cancel := ic.Merge(ctx, s.baseCtx)
		defer cancel()

		operation := fmt.Sprintf("/%s/%s", serviceName, method)

		reqHeader := natsrpc.CallHeader(ctx)
		if reqHeader == nil {
			reqHeader = make(map[string]string)
		}

		tr := &Transport{
			endpoint:    subjectEndpoint(s.namespace, serviceName, s.id),
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
		// 把中间件写入 tr.ReplyHeader() 的内容下推到 natsrpc，由其随响应回传。
		// 成功/失败两路都做，使业务错误也能携带 reply header。
		if keys := tr.replyHeader.Keys(); len(keys) > 0 {
			rh := make(map[string]string, len(keys))
			for _, k := range keys {
				rh[k] = tr.replyHeader.Get(k)
			}
			_ = natsrpc.SetReplyHeader(ctx, rh)
		}
		if err != nil {
			// 交给 natsrpc 一个其文本为 protojson 编码 Status 的 error；
			// 它会被放进 _ns_error header，客户端的 wrapper 再把它解码
			// 还原成完整的 *errors.Error。
			return nil, errors.New(EncodeError(err))
		}
		return reply, nil
	}
}

// serviceRef 是一个已注册服务的句柄。在 Start 之前它只是一个占位符；
// 之后它会委托给真实的 natsrpc 服务。
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

// Name 返回服务名。
func (r *serviceRef) Name() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.real != nil {
		return r.real.Name()
	}
	return r.name
}

// Close 在服务处于活跃状态时取消其订阅。
func (r *serviceRef) Close() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.real != nil {
		return r.real.Close()
	}
	return false
}
