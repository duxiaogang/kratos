package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/byebyebruce/natsrpc"
	"github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

var _ natsrpc.ClientInterface = (*Client)(nil)

// Client 是 NATS RPC 传输层客户端。
//
// NATS 在 broker 层提供了位置透明性以及组内的负载均衡，因此这里不像 gRPC
// 传输层那样有 selector/discovery 解析器：客户端按服务名寻址，由 broker
// 负责路由。
type Client struct {
	client     *natsrpc.Client
	conn       *nats.Conn
	timeout    time.Duration //request timeout
	namespace  string
	ownConn    bool
	middleware []middleware.Middleware
}

// Dial 创建一个 NATS 客户端连接。
func Dial(ctx context.Context, opts ...ClientOption) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	options := &clientOptions{
		address: nats.DefaultURL,
		timeout: 2 * time.Second,
		ownConn: true,
		encoder: ProtoEncoder{},
	}
	for _, o := range opts {
		o(options)
	}

	c := &Client{
		timeout:    options.timeout,
		namespace:  options.namespace,
		ownConn:    options.ownConn,
		middleware: options.middleware,
	}

	if options.conn != nil {
		c.conn = options.conn
		c.ownConn = false
	} else {
		conn, err := connectWithContext(ctx, options.address, options.natsOpts...)
		if err != nil {
			return nil, fmt.Errorf("[NATS] failed to connect: %w", err)
		}
		c.conn = conn
		c.ownConn = true
	}

	clientOpts := []natsrpc.ClientOption{
		natsrpc.WithClientEncoder(options.encoder),
	}
	if options.namespace != "" {
		clientOpts = append(clientOpts, natsrpc.WithClientNamespace(options.namespace))
	}
	c.client = natsrpc.NewClient(c.conn, clientOpts...)

	return c, nil
}

// connectWithContext adapts nats.Connect, which does not accept a context, to
// the context-aware Dial API. If the context is canceled while connecting, a
// late connection is closed as soon as nats.Connect returns.
func connectWithContext(ctx context.Context, address string, opts ...nats.Option) (*nats.Conn, error) {
	type result struct {
		conn *nats.Conn
		err  error
	}

	connected := make(chan result, 1)
	go func() {
		conn, err := nats.Connect(address, opts...)
		connected <- result{conn: conn, err: err}
	}()

	select {
	case res := <-connected:
		if err := ctx.Err(); err != nil {
			if res.conn != nil {
				res.conn.Close()
			}
			return nil, err
		}
		if res.err != nil {
			return nil, res.err
		}
		return res.conn, nil
	case <-ctx.Done():
		go func() {
			res := <-connected
			if res.conn != nil {
				res.conn.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

// Publish 发布一条消息，不等待响应。
// 它实现了 natsrpc.ClientInterface。
// 调用方不得传入 natsrpc.WithCallHeader 或 natsrpc.WithCallReplyHeader；请求
// header 由客户端中间件通过 Transport.RequestHeader 写入，Publish 不产生响应。
func (c *Client) Publish(service, method string, req interface{}, opt ...natsrpc.CallOption) error {
	operation := fmt.Sprintf("/%s/%s", service, method)
	tr := &Transport{
		// TODO: 客户端 endpoint 暂不包含 call id；后续等 natsrpc 代码生成支持后再显式传入 service id。
		endpoint:    subjectEndpoint(c.namespace, service),
		operation:   operation,
		reqHeader:   make(headerCarrier),
		replyHeader: make(headerCarrier),
	}
	ctx := transport.NewClientContext(context.Background(), tr)

	h := func(ctx context.Context, req any) (any, error) {
		// 从 ctx 重新取 transport，而不是用闭包捕获的 tr：中间件可能在
		// 链路中替换掉 context 里的 transport，header 应以最终的为准。
		callOpts := c.withHeader(transportFromClient(ctx), opt)
		err := c.client.Publish(service, method, req, callOpts...)
		return nil, decodeRemoteError(err)
	}
	if len(c.middleware) > 0 {
		h = middleware.Chain(c.middleware...)(h)
	}

	_, err := h(ctx, req)
	return err
}

// Request 发送一个请求并等待响应。
// 它实现了 natsrpc.ClientInterface。
// 调用方不得传入 natsrpc.WithCallHeader 或 natsrpc.WithCallReplyHeader；请求和
// 响应 header 由客户端中间件通过 Transport.RequestHeader/ReplyHeader 管理。
func (c *Client) Request(ctx context.Context, service, method string, req interface{}, rep interface{}, opt ...natsrpc.CallOption) error {
	// 当调用方未设置 deadline 时，应用客户端的超时时间。
	if _, ok := ctx.Deadline(); !ok && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	operation := fmt.Sprintf("/%s/%s", service, method)
	tr := &Transport{
		// TODO: 客户端 endpoint 暂不包含 call id；后续等 natsrpc 代码生成支持后再显式传入 service id。
		endpoint:    subjectEndpoint(c.namespace, service),
		operation:   operation,
		reqHeader:   make(headerCarrier),
		replyHeader: make(headerCarrier),
	}
	ctx = transport.NewClientContext(ctx, tr)

	h := func(ctx context.Context, req any) (any, error) {
		tr := transportFromClient(ctx)
		callOpts := c.withHeader(tr, opt)
		// 追加 transport 自己的 reply header 容器。WithCallReplyHeader 是
		// transport 保留选项，调用方不得传入；这里新建一份 slice，避免污染
		// 调用方传入的 opt。
		var rh map[string]string
		merged := make([]natsrpc.CallOption, 0, len(callOpts)+1)
		merged = append(merged, callOpts...)
		merged = append(merged, natsrpc.WithCallReplyHeader(&rh))

		err := c.client.Request(ctx, service, method, req, rep, merged...)
		// 把服务端回传的响应 header 填进 transport，供客户端中间件读取。
		if len(rh) > 0 && tr != nil {
			for k, v := range rh {
				tr.replyHeader.Set(k, v)
			}
		}
		return rep, decodeRemoteError(err)
	}
	if len(c.middleware) > 0 {
		h = middleware.Chain(c.middleware...)(h)
	}

	_, err := h(ctx, req)
	return err
}

// decodeRemoteError 只还原 NATS 响应中由服务端 encodeError 生成的错误。
// 其他错误属于本地调用链，必须原样返回以保留 errors.Is/As 和 context
// 取消语义。
func decodeRemoteError(err error) error {
	if err == nil {
		return nil
	}
	if decoded, ok := decodeEncodedError(err.Error()); ok {
		return decoded
	}
	return err
}

func transportFromClient(ctx context.Context) *Transport {
	if tr, ok := transport.FromClientContext(ctx); ok {
		if t, ok := tr.(*Transport); ok {
			return t
		}
	}
	return nil
}

// withHeader 为本次调用构建 natsrpc 的 call options，并追加中间件写入到
// transport 上的所有 header。调用方不得在 base 中传入 WithCallHeader；该
// 选项由 transport 保留。每次调用都返回一个全新的 slice，因此重试时绝不会
// 累积重复的 option。
func (c *Client) withHeader(tr *Transport, base []natsrpc.CallOption) []natsrpc.CallOption {
	if tr == nil {
		return base
	}
	keys := tr.reqHeader.Keys()
	if len(keys) == 0 {
		return base
	}
	header := make(map[string]string, len(keys))
	for _, k := range keys {
		header[k] = tr.reqHeader.Get(k)
	}
	out := make([]natsrpc.CallOption, 0, len(base)+1)
	out = append(out, base...)
	out = append(out, natsrpc.WithCallHeader(header))
	return out
}

// Close 在客户端拥有连接时关闭该连接。
func (c *Client) Close() error {
	if c.ownConn && c.conn != nil {
		c.conn.Close()
	}
	return nil
}
