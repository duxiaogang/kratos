package main

import (
	"context"
	"fmt"

	"github.com/byebyebruce/natsrpc"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

type job struct {
	ctx  context.Context
	name string
}
type result struct {
	reply *api.HelloReply
	err   error
}

func run(ctx context.Context, d *demo.Demo) error {
	jobs := make(chan job, 1)
	srv := nats.NewServer(nats.Address(d.URL))
	_, err := api.RegisterGreeterNRServer(srv, &demo.Greeter{Hello: func(ctx context.Context, req *api.HelloRequest) (*api.HelloReply, error) {
		if req.Name == "barrier" {
			return &api.HelloReply{Message: "ready"}, nil
		}
		jobs <- job{ctx: ctx, name: req.Name}
		return nil, natsrpc.ErrReplyLater
	}}, natsrpc.WithServiceSingleGoroutine())
	if err != nil {
		return err
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
	results := make(chan result, 1)
	go func() {
		reply, err := greeter.SayHello(ctx, &api.HelloRequest{Name: "reply"})
		results <- result{reply: reply, err: err}
	}()
	pending, err := demo.Receive(ctx, jobs)
	if err != nil {
		return err
	}
	// 同一请求订阅串行处理：barrier 能返回，证明前一个 handler 已经返回。
	// 实际应用中这里可以是队列消费者；无需用 Sleep 猜测 handler 的状态。
	if _, err := greeter.SayHello(ctx, &api.HelloRequest{Name: "barrier"}); err != nil {
		return err
	}
	if err := pending.ctx.Err(); err != nil {
		return fmt.Errorf("context ended before deferred reply: %w", err)
	}
	if err := natsrpc.Reply(pending.ctx, &api.HelloReply{Message: "Hello, " + pending.name + " later!"}, nil); err != nil {
		return err
	}
	got, err := demo.Receive(ctx, results)
	if err != nil {
		return err
	}
	if got.err != nil {
		return got.err
	}
	fmt.Fprintln(d.Out, got.reply.Message)
	return nil
}

func main() { demo.Main(run) }
