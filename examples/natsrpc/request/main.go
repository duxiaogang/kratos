package main

import (
	"context"
	"fmt"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

func run(ctx context.Context, d *demo.Demo) error {
	srv := nats.NewServer(nats.Address(d.URL))
	ref, err := api.RegisterGreeterNRServer(srv, &demo.Greeter{Hello: demo.Hello})
	if err != nil {
		return err
	}
	d.Cleanup(func() error { ref.Close(); return nil })
	if err := d.Start(ctx, srv); err != nil {
		return err
	}

	client, err := nats.Dial(ctx, nats.WithAddress(d.URL))
	if err != nil {
		return err
	}
	d.Cleanup(client.Close)
	reply, err := api.NewGreeterNRClient(client).SayHello(ctx, &api.HelloRequest{Name: "request"})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(d.Out, "request: %s\n", reply.Message)
	return err
}

func main() { demo.Main(run) }
