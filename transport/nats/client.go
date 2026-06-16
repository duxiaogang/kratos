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
		conn, err := nats.Connect(options.address, options.natsOpts...) //nats.Connect不使用ctx?
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

// Publish 发布一条消息，不等待响应。
// 它实现了 natsrpc.ClientInterface。
func (c *Client) Publish(service, method string, req interface{}, opt ...natsrpc.CallOption) error {
	operation := fmt.Sprintf("/%s/%s", service, method)
	tr := &Transport{
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
		return nil, c.client.Publish(service, method, req, callOpts...)
	}
	if len(c.middleware) > 0 {
		h = middleware.Chain(c.middleware...)(h)
	}

	_, err := h(ctx, req)
	if err != nil {
		return DecodeError(err.Error()) //todo: 所有error一定是来自server？也就是一定是encoded error?
	}
	return nil
}

// Request 发送一个请求并等待响应。
// 它实现了 natsrpc.ClientInterface。
func (c *Client) Request(ctx context.Context, service, method string, req interface{}, rep interface{}, opt ...natsrpc.CallOption) error {
	// 当调用方未设置 deadline 时，应用客户端的超时时间。
	if _, ok := ctx.Deadline(); !ok && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	operation := fmt.Sprintf("/%s/%s", service, method)
	tr := &Transport{
		endpoint:    subjectEndpoint(c.namespace, service),
		operation:   operation,
		reqHeader:   make(headerCarrier),
		replyHeader: make(headerCarrier),
	}
	ctx = transport.NewClientContext(ctx, tr)

	h := func(ctx context.Context, req any) (any, error) {
		callOpts := c.withHeader(transportFromClient(ctx), opt)
		err := c.client.Request(ctx, service, method, req, rep, callOpts...)
		// natsrpc v0.7.0 的 Request 只回传解码后的 rep，不暴露响应消息的
		// header，因此这里无法把 reply header 填充到 tr.ReplyHeader()。
		// 若要支持，需要 natsrpc 在客户端暴露响应消息的 header。
		return rep, err
	}
	if len(c.middleware) > 0 {
		h = middleware.Chain(c.middleware...)(h)
	}

	_, err := h(ctx, req)
	if err != nil {
		// 还原服务端编码的结构化 Kratos 错误。
		return DecodeError(err.Error()) //todo: 所有error一定是来自server？也就是一定是encoded error?
	}
	return nil
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
// transport 上的所有 header。每次调用都返回一个全新的 slice，因此重试时
// 绝不会累积重复的 option。
func (c *Client) withHeader(tr *Transport, base []natsrpc.CallOption) []natsrpc.CallOption {
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
