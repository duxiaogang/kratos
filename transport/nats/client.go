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

// Client is a NATS RPC transport client.
//
// NATS provides location transparency and in-group load balancing at the
// broker level, so there is no selector/discovery resolver here as in the gRPC
// transport: a client addresses a service by name and the broker routes it.
type Client struct {
	client     *natsrpc.Client
	conn       *nats.Conn
	endpoint   string        //todo: 别叫endpoint了，叫address之类吧，endpoint中应该包含namespace/id
	timeout    time.Duration //request timeout
	namespace  string
	ownConn    bool
	middleware []middleware.Middleware
}

// Dial creates a NATS client connection.
func Dial(ctx context.Context, opts ...ClientOption) (*Client, error) {
	options := &clientOptions{
		endpoint: nats.DefaultURL,
		timeout:  2 * time.Second,
		ownConn:  true,
		encoder:  ProtoEncoder{},
	}
	for _, o := range opts {
		o(options)
	}

	c := &Client{
		endpoint:   options.endpoint,
		timeout:    options.timeout,
		namespace:  options.namespace,
		ownConn:    options.ownConn,
		middleware: options.middleware,
	}

	if options.conn != nil {
		c.conn = options.conn
		c.ownConn = false
	} else {
		conn, err := nats.Connect(options.endpoint, options.natsOpts...) //nats.Connect不使用ctx?
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

// Publish publishes a message without waiting for a response.
// It implements natsrpc.ClientInterface.
func (c *Client) Publish(service, method string, req interface{}, opt ...natsrpc.CallOption) error {
	operation := fmt.Sprintf("/%s/%s", service, method)
	tr := &Transport{
		endpoint:    c.endpoint,
		operation:   operation,
		reqHeader:   make(headerCarrier),
		replyHeader: make(headerCarrier),
	}
	ctx := transport.NewClientContext(context.Background(), tr)

	h := func(ctx context.Context, req any) (any, error) {
		//todo: tr不要用捕获的，重新从ctx里拿
		callOpts := c.withHeader(tr, opt)
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

// Request sends a request and waits for the response.
// It implements natsrpc.ClientInterface.
func (c *Client) Request(ctx context.Context, service, method string, req interface{}, rep interface{}, opt ...natsrpc.CallOption) error {
	// Apply the client timeout when the caller has not set a deadline.
	if _, ok := ctx.Deadline(); !ok && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	operation := fmt.Sprintf("/%s/%s", service, method)
	tr := &Transport{
		endpoint:    c.endpoint,
		operation:   operation,
		reqHeader:   make(headerCarrier),
		replyHeader: make(headerCarrier),
	}
	ctx = transport.NewClientContext(ctx, tr)

	h := func(ctx context.Context, req any) (any, error) {
		//todo: tr不要用捕获的，重新从ctx里拿
		callOpts := c.withHeader(tr, opt)
		err := c.client.Request(ctx, service, method, req, rep, callOpts...)
		//todo: 这里是否也应该获取reply header？
		return rep, err
	}
	if len(c.middleware) > 0 {
		h = middleware.Chain(c.middleware...)(h)
	}

	_, err := h(ctx, req)
	if err != nil {
		// Restore the structured Kratos error encoded by the server.
		return DecodeError(err.Error()) //todo: 所有error一定是来自server？也就是一定是encoded error?
	}
	return nil
}

// withHeader builds the natsrpc call options for this invocation, appending any
// headers that middleware wrote onto the transport. A fresh slice is returned
// each call so retries never accumulate duplicate options.
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

// Close closes the client connection if the client owns it.
func (c *Client) Close() error {
	if c.ownConn && c.conn != nil {
		c.conn.Close()
	}
	return nil
}
