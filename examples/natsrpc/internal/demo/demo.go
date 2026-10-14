// Package demo contains only the lifecycle plumbing shared by the examples.
package demo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/byebyebruce/natsrpc"
	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

// Run is used unchanged by go run and go test.
type Run func(context.Context, *Demo) error

// Demo owns the resources of one run. Cleanup functions run in reverse order.
type Demo struct {
	URL     string
	Out     io.Writer
	cleanup []func() error
}

func (d *Demo) Cleanup(fn func() error) { d.cleanup = append(d.cleanup, fn) }

func (d *Demo) Close() error {
	var err error
	for i := len(d.cleanup) - 1; i >= 0; i-- {
		err = errors.Join(err, d.cleanup[i]())
	}
	d.cleanup = nil
	return err
}

// Main runs one example against NATS_URL (127.0.0.1:4222 by default).
func Main(run Run) {
	address := os.Getenv("NATS_URL")
	if address == "" {
		address = natsgo.DefaultURL
	}
	d := &Demo{URL: address, Out: os.Stdout}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := run(ctx, d)
	err = errors.Join(err, d.Close())
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Start starts one server and waits until its subscriptions are usable. The
// readiness service uses a generated, per-run name and is removed before this
// method returns. probeOptions must match the server namespace/encoder.
func (d *Demo) Start(ctx context.Context, srv *nats.Server, probeOptions ...nats.ClientOption) error {
	name := "example_ready_" + uuid.NewString()
	ref, err := srv.Register(natsrpc.ServiceDesc{
		ServiceName: name,
		Methods: []natsrpc.MethodDesc{{
			MethodName:     "Ready",
			RequestFactory: func() any { return &api.HelloRequest{} },
			Handler:        func(any, context.Context, any) (any, error) { return &api.HelloReply{}, nil },
		}},
	}, nil, natsrpc.WithServiceInterceptor(func(ctx context.Context, _ string, req any, next natsrpc.Invoker) (any, error) {
		return next(ctx, req)
	}))
	if err != nil {
		return err
	}
	defer ref.Close()

	done := make(chan error, 1)
	go func() { done <- srv.Start(ctx) }()
	d.Cleanup(func() error {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := srv.Stop(stopCtx)
		select {
		case startErr := <-done:
			return errors.Join(err, startErr)
		case <-stopCtx.Done():
			return errors.Join(err, stopCtx.Err())
		}
	})
	opts := append([]nats.ClientOption{nats.WithAddress(d.URL), nats.WithTimeout(200 * time.Millisecond)}, probeOptions...)
	probe, err := nats.Dial(ctx, opts...)
	if err != nil {
		return fmt.Errorf("connect readiness probe: %w", err)
	}
	defer probe.Close()
	for {
		reply := &api.HelloReply{}
		err = probe.Request(ctx, name, "Ready", &api.HelloRequest{}, reply)
		if err == nil {
			return nil
		}
		select {
		case startErr := <-done:
			done <- startErr // Let cleanup observe Start's result too.
			return fmt.Errorf("server exited before becoming ready: %v", startErr)
		case <-ctx.Done():
			return fmt.Errorf("wait for subscriptions: %w (last probe: %v)", ctx.Err(), err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Receive gives every asynchronous assertion a bounded wait.
func Receive[T any](ctx context.Context, ch <-chan T) (T, error) {
	select {
	case value := <-ch:
		return value, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// Greeter lets each example implement just the method it is demonstrating.
// Unexpected calls fail instead of silently succeeding.
type Greeter struct {
	Hello        func(context.Context, *api.HelloRequest) (*api.HelloReply, error)
	Notification func(context.Context, *api.NotifyRequest) (*api.Empty, error)
}

func (s *Greeter) SayHello(ctx context.Context, req *api.HelloRequest) (*api.HelloReply, error) {
	if s.Hello == nil {
		return nil, errors.New("SayHello is not used in this example")
	}
	return s.Hello(ctx, req)
}

func (s *Greeter) Notify(ctx context.Context, req *api.NotifyRequest) (*api.Empty, error) {
	if s.Notification == nil {
		return nil, errors.New("Notify is not used in this example")
	}
	return s.Notification(ctx, req)
}

func Hello(_ context.Context, req *api.HelloRequest) (*api.HelloReply, error) {
	return &api.HelloReply{Message: "Hello, " + req.Name + "!"}, nil
}
