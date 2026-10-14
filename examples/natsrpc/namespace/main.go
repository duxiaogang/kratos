package main

import (
	"context"
	"errors"
	"fmt"

	natsgo "github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

func run(ctx context.Context, d *demo.Demo) error {
	// 两个 namespace 中注册同名服务，回复带上实际处理请求的 namespace。
	for _, ns := range []string{"tenant-a", "tenant-b"} {
		srv := nats.NewServer(nats.Address(d.URL), nats.Namespace(ns))
		_, err := api.RegisterGreeterNRServer(srv, &demo.Greeter{Hello: func(ctx context.Context, req *api.HelloRequest) (*api.HelloReply, error) {
			tr, ok := transport.FromServerContext(ctx)
			if !ok || tr.Endpoint() != ns+"."+api.Greeter_NRServiceName {
				return nil, fmt.Errorf("wrong server endpoint")
			}
			return &api.HelloReply{Message: ns + ": " + req.Name}, nil
		}})
		if err != nil {
			return err
		}
		if err := d.Start(ctx, srv, nats.WithNamespace(ns)); err != nil {
			return err
		}
		endpoint, err := srv.Endpoint()
		if err != nil {
			return err
		}
		if endpoint.Query().Get("namespace") != ns {
			return fmt.Errorf("namespace missing from advertised endpoint: %s", endpoint)
		}
	}
	for _, ns := range []string{"tenant-a", "tenant-b", "tenant-missing"} {
		client, err := nats.Dial(ctx, nats.WithAddress(d.URL), nats.WithNamespace(ns))
		if err != nil {
			return err
		}
		d.Cleanup(client.Close)
		reply, err := api.NewGreeterNRClient(client).SayHello(ctx, &api.HelloRequest{Name: "hello"})
		if ns == "tenant-missing" {
			if !errors.Is(err, natsgo.ErrNoResponders) {
				return fmt.Errorf("missing namespace: %v", err)
			}
			fmt.Fprintln(d.Out, "missing namespace: no responders")
			continue
		}
		if err != nil {
			return err
		}
		if reply.Message != ns+": hello" {
			return fmt.Errorf("cross-namespace reply: %q", reply.Message)
		}
		fmt.Fprintln(d.Out, reply.Message)
	}
	return nil
}

func main() { demo.Main(run) }
