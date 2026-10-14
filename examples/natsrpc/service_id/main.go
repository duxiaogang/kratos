package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/byebyebruce/natsrpc"
	natsgo "github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

func run(ctx context.Context, d *demo.Demo) error {
	received := make(chan string, 4)
	srv := nats.NewServer(nats.Address(d.URL))
	for _, id := range []string{"greeter-1", "greeter-2"} {
		svc := &demo.Greeter{
			Hello: func(ctx context.Context, _ *api.HelloRequest) (*api.HelloReply, error) {
				tr, ok := transport.FromServerContext(ctx)
				if !ok || tr.Endpoint() != api.Greeter_NRServiceName+"."+id {
					return nil, fmt.Errorf("wrong service endpoint")
				}
				return &api.HelloReply{Message: id}, nil
			},
			Notification: func(_ context.Context, req *api.NotifyRequest) (*api.Empty, error) {
				received <- id + ": " + req.Content
				return &api.Empty{}, nil
			},
		}
		ref, err := api.RegisterGreeterNRServer(srv, svc, natsrpc.WithServiceID(id))
		if err != nil {
			return err
		}
		if ref.Name() != api.Greeter_NRServiceName+"."+id {
			return fmt.Errorf("service name: %s", ref.Name())
		}
	}
	if err := d.Start(ctx, srv); err != nil {
		return err
	}
	client, err := nats.Dial(ctx, nats.WithAddress(d.URL))
	if err != nil {
		return err
	}
	d.Cleanup(client.Close)
	greeter := api.NewGreeterNRClient(client)
	for _, id := range []string{"greeter-1", "greeter-2"} {
		// WithCallID 同时支持 Request 和 Publish；不同 id 之间不会广播。
		reply, err := greeter.SayHello(ctx, &api.HelloRequest{}, natsrpc.WithCallID(id))
		if err != nil {
			return err
		}
		if reply.Message != id {
			return fmt.Errorf("wrong instance: %q", reply.Message)
		}
		if err := greeter.Notify(&api.NotifyRequest{Content: "targeted"}, natsrpc.WithCallID(id)); err != nil {
			return err
		}
		notification, err := demo.Receive(ctx, received)
		if err != nil {
			return err
		}
		if notification != id+": targeted" {
			return fmt.Errorf("wrong notification target: %s", notification)
		}
		fmt.Fprintf(d.Out, "request: %s; publish: %s\n", reply.Message, notification)
	}
	// 省略 id 不会自动选择这些带 id 的实例。
	for _, id := range []string{"", "missing"} {
		_, err := greeter.SayHello(ctx, &api.HelloRequest{}, natsrpc.WithCallID(id))
		if !errors.Is(err, natsgo.ErrNoResponders) {
			return fmt.Errorf("unknown id %q: %v", id, err)
		}
	}
	fmt.Fprintln(d.Out, "empty/missing id: no responders")
	return nil
}

func main() { demo.Main(run) }
