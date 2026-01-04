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

// Client is a NATS client wrapper.
type Client struct {
	client     *natsrpc.Client
	conn       *nats.Conn
	endpoint   string
	timeout    time.Duration
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
	}
	for _, o := range opts {
		o(options)
	}

	c := &Client{
		endpoint:  options.endpoint,
		timeout:   options.timeout,
		namespace: options.namespace,
		ownConn:   options.ownConn,
	}

	// Use existing connection or create new one
	if options.conn != nil {
		c.conn = options.conn
		c.ownConn = false
	} else {
		conn, err := nats.Connect(options.endpoint, options.natsOpts...)
		if err != nil {
			return nil, fmt.Errorf("[NATS] failed to connect: %w", err)
		}
		c.conn = conn
		c.ownConn = true
	}

	// Create natsrpc client
	clientOpts := []natsrpc.ClientOption{}
	if options.namespace != "" {
		clientOpts = append(clientOpts, natsrpc.WithClientNamespace(options.namespace))
	}
	if options.encoder != nil {
		if enc, ok := options.encoder.(natsrpc.Encoder); ok {
			clientOpts = append(clientOpts, natsrpc.WithClientEncoder(enc))
		}
	}
	c.client = natsrpc.NewClient(c.conn, clientOpts...)

	return c, nil
}

// NewClient creates a NATS client with an existing connection.
func NewClient(conn *nats.Conn, opts ...ClientOption) *Client {
	options := &clientOptions{
		timeout: 2 * time.Second,
		ownConn: false,
	}
	for _, o := range opts {
		o(options)
	}

	clientOpts := []natsrpc.ClientOption{}
	if options.namespace != "" {
		clientOpts = append(clientOpts, natsrpc.WithClientNamespace(options.namespace))
	}
	if options.encoder != nil {
		if enc, ok := options.encoder.(natsrpc.Encoder); ok {
			clientOpts = append(clientOpts, natsrpc.WithClientEncoder(enc))
		}
	}

	return &Client{
		client:    natsrpc.NewClient(conn, clientOpts...),
		conn:      conn,
		endpoint:  conn.ConnectedUrl(),
		timeout:   options.timeout,
		namespace: options.namespace,
		ownConn:   false,
	}
}

// Publish publishes a message without waiting for response.
// This method implements natsrpc.ClientInterface.
func (c *Client) Publish(service, method string, req interface{}, opt ...natsrpc.CallOption) error {
	// Build operation name
	operation := fmt.Sprintf("/%s/%s", service, method)

	// Create transport for middleware
	tr := &Transport{
		endpoint:    c.endpoint,
		operation:   operation,
		reqHeader:   make(headerCarrier),
		replyHeader: make(headerCarrier),
	}

	// Inject client transport context
	ctx := transport.NewClientContext(context.Background(), tr)

	// Build handler
	h := func(ctx context.Context, req any) (any, error) {
		// Extract headers from transport and add to call options
		header := make(map[string]string)
		for _, k := range tr.reqHeader.Keys() {
			header[k] = tr.reqHeader.Get(k)
		}
		if len(header) > 0 {
			opt = append(opt, natsrpc.WithCallHeader(header))
		}

		return nil, c.client.Publish(service, method, req, opt...)
	}

	// Apply middleware
	if len(c.middleware) > 0 {
		h = middleware.Chain(c.middleware...)(h)
	}

	_, err := h(ctx, req)
	return err
}

// Request sends a request and waits for response.
// This method implements natsrpc.ClientInterface.
func (c *Client) Request(ctx context.Context, service, method string, req interface{}, rep interface{}, opt ...natsrpc.CallOption) error {
	// Build operation name
	operation := fmt.Sprintf("/%s/%s", service, method)

	// Create transport for middleware
	tr := &Transport{
		endpoint:    c.endpoint,
		operation:   operation,
		reqHeader:   make(headerCarrier),
		replyHeader: make(headerCarrier),
	}

	// Inject client transport context
	ctx = transport.NewClientContext(ctx, tr)

	// Build handler
	h := func(ctx context.Context, req any) (any, error) {
		// Extract headers from transport and add to call options
		header := make(map[string]string)
		for _, k := range tr.reqHeader.Keys() {
			header[k] = tr.reqHeader.Get(k)
		}
		if len(header) > 0 {
			opt = append(opt, natsrpc.WithCallHeader(header))
		}

		err := c.client.Request(ctx, service, method, req, rep, opt...)
		return rep, err
	}

	// Apply middleware
	if len(c.middleware) > 0 {
		h = middleware.Chain(c.middleware...)(h)
	}

	_, err := h(ctx, req)
	return err
}

// Close closes the client connection.
func (c *Client) Close() error {
	if c.ownConn && c.conn != nil {
		c.conn.Close()
	}
	return nil
}

// GetClient returns the underlying natsrpc.Client.
func (c *Client) GetClient() *natsrpc.Client {
	return c.client
}

// GetConn returns the underlying NATS connection.
func (c *Client) GetConn() *nats.Conn {
	return c.conn
}
