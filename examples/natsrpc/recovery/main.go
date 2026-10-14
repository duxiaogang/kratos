package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

func run(ctx context.Context, d *demo.Demo) error {
	recovered := make(chan any, 2)
	srv := nats.NewServer(nats.Address(d.URL), nats.RecoveryHandler(func(value any) { recovered <- value }))
	_, err := api.RegisterGreeterNRServer(srv, &demo.Greeter{
		Hello: func(ctx context.Context, req *api.HelloRequest) (*api.HelloReply, error) {
			if req.Name == "panic" {
				panic("demo panic")
			}
			return demo.Hello(ctx, req)
		},
		Notification: func(context.Context, *api.NotifyRequest) (*api.Empty, error) { panic("demo panic") },
	})
	if err != nil {
		return err
	}
	if err := d.Start(ctx, srv); err != nil {
		return err
	}
	client, err := nats.Dial(ctx, nats.WithAddress(d.URL), nats.WithTimeout(250*time.Millisecond))
	if err != nil {
		return err
	}
	d.Cleanup(client.Close)
	greeter := api.NewGreeterNRClient(client)
	if err := greeter.Notify(&api.NotifyRequest{}); err != nil {
		return err
	}
	value, err := demo.Receive(ctx, recovered)
	if err != nil {
		return err
	}
	if value != "demo panic" {
		return fmt.Errorf("panic value: %v", value)
	}
	fmt.Fprintln(d.Out, "publish panic: recovered")
	// 原生 RecoveryHandler 只观察 panic，不会自动发送 RPC 错误响应。
	_, err = greeter.SayHello(ctx, &api.HelloRequest{Name: "panic"})
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("expected request deadline, got %v", err)
	}
	value, err = demo.Receive(ctx, recovered)
	if err != nil {
		return err
	}
	if value != "demo panic" {
		return fmt.Errorf("panic value: %v", value)
	}
	fmt.Fprintln(d.Out, "request panic: client deadline exceeded")
	reply, err := greeter.SayHello(ctx, &api.HelloRequest{Name: "still running"})
	if err != nil {
		return err
	}
	fmt.Fprintln(d.Out, reply.Message)
	return nil
}

func main() { demo.Main(run) }
