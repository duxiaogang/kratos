package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

func run(ctx context.Context, d *demo.Demo) error {
	events := make(chan string, 7)
	record := func(server bool) middleware.Middleware {
		return func(next middleware.Handler) middleware.Handler {
			return func(ctx context.Context, req any) (any, error) {
				tr, ok := transport.FromClientContext(ctx)
				if server {
					tr, ok = transport.FromServerContext(ctx)
				}
				if !ok || tr.Kind() != transport.KindNATS || tr.Operation() != "/api.Greeter/SayHello" {
					return nil, fmt.Errorf("missing or incorrect transport context")
				}
				if server {
					if got := tr.RequestHeader().Get("x-example"); got != "client" {
						return nil, fmt.Errorf("request header = %q, want client", got)
					}
					events <- "server before (request-header=client)"
				} else {
					tr.RequestHeader().Set("x-example", "client")
					events <- "client before (request-header=client)"
				}
				reply, err := next(ctx, req)
				if server {
					tr.ReplyHeader().Set("x-example", "server")
					events <- "server after (reply-header=server)"
				} else {
					if got := tr.ReplyHeader().Get("x-example"); got != "server" {
						return nil, fmt.Errorf("reply header = %q, want server", got)
					}
					events <- "client after (reply-header=server)"
				}
				return reply, err
			}
		}
	}
	srv := nats.NewServer(nats.Address(d.URL), nats.Middleware(record(true)))
	_, err := api.RegisterGreeterNRServer(srv, &demo.Greeter{Hello: func(ctx context.Context, req *api.HelloRequest) (*api.HelloReply, error) {
		events <- "handler"
		return demo.Hello(ctx, req)
	}})
	if err != nil {
		return err
	}
	if err := d.Start(ctx, srv); err != nil {
		return err
	}
	client, err := nats.Dial(ctx, nats.WithAddress(d.URL), nats.WithMiddleware(record(false)))
	if err != nil {
		return err
	}
	d.Cleanup(client.Close)
	if _, err := api.NewGreeterNRClient(client).SayHello(ctx, &api.HelloRequest{}); err != nil {
		return err
	}
	order := make([]string, 0, 5)
	for range 5 {
		event, err := demo.Receive(ctx, events)
		if err != nil {
			return err
		}
		order = append(order, event)
	}
	fmt.Fprintln(d.Out, strings.Join(order, " -> "))
	return nil
}

func main() { demo.Main(run) }
