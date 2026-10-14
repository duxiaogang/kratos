package main

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/byebyebruce/natsrpc"
	natsgo "github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

// This example puts the lifecycle options in one place. Feature examples keep
// their setup small, while this one shows an externally owned connection, an
// advertised endpoint independent from the broker address, and connection
// options applied both to a server and a client.
func run(ctx context.Context, d *demo.Demo) error {
	shared, err := natsgo.Connect(d.URL, natsgo.NoReconnect())
	if err != nil {
		return err
	}
	d.Cleanup(func() error { shared.Close(); return nil })
	advertised, err := url.Parse("nats://advertised.example:4222")
	if err != nil {
		return err
	}
	owned := nats.NewServer(
		nats.Address(d.URL),
		nats.Endpoint(advertised),
		nats.Timeout(50*time.Millisecond),
		nats.NatsOptions(natsgo.NoReconnect()),
		nats.ErrorHandler(func(any) {}),
	)
	register := func(srv *nats.Server, id string) error {
		_, err := api.RegisterGreeterNRServer(srv, &demo.Greeter{Hello: func(ctx context.Context, req *api.HelloRequest) (*api.HelloReply, error) {
			if req.Name == "timeout" {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return demo.Hello(ctx, req)
		}}, natsrpc.WithServiceID(id))
		return err
	}
	if err := register(owned, "owned"); err != nil {
		return err
	}
	if err := d.Start(ctx, owned); err != nil {
		return err
	}

	// Connection makes the server use the caller-owned connection. It is kept
	// separate from owned so NatsOptions above is exercised by nats.Connect.
	sharedServer := nats.NewServer(nats.Address(d.URL), nats.Connection(shared))
	if err := register(sharedServer, "shared"); err != nil {
		return err
	}
	if err := d.Start(ctx, sharedServer); err != nil {
		return err
	}

	// WithEndpoint is retained as a compatibility alias for WithAddress. The
	// endpoint option above only changes what a registry would advertise.
	client, err := nats.Dial(ctx,
		nats.WithEndpoint(d.URL),
		nats.WithTimeout(time.Second),
		nats.WithNatsOptions(natsgo.NoReconnect()),
	)
	if err != nil {
		return err
	}
	d.Cleanup(client.Close)
	greeter := api.NewGreeterNRClient(client)
	started := time.Now()
	if _, err := greeter.SayHello(ctx, &api.HelloRequest{Name: "timeout"}, natsrpc.WithCallID("owned")); err == nil {
		return fmt.Errorf("expected server timeout")
	} else if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		return fmt.Errorf("server timeout took %s", elapsed)
	}
	reply, err := greeter.SayHello(ctx, &api.HelloRequest{Name: "options"}, natsrpc.WithCallID("owned"))
	if err != nil {
		return err
	}

	sharedClient, err := nats.Dial(ctx, nats.WithAddress(d.URL), nats.WithConnection(shared), nats.WithTimeout(time.Second))
	if err != nil {
		return err
	}
	d.Cleanup(sharedClient.Close)
	sharedReply, err := api.NewGreeterNRClient(sharedClient).SayHello(ctx, &api.HelloRequest{Name: "shared"}, natsrpc.WithCallID("shared"))
	if err != nil {
		return err
	}
	if sharedReply.Message != "Hello, shared!" {
		return errors.New(500, "BAD_REPLY", "unexpected shared connection reply")
	}
	if err := sharedServer.Stop(ctx); err != nil {
		return err
	}
	if shared.IsClosed() {
		return fmt.Errorf("server closed caller-owned connection")
	}
	ep, err := owned.Endpoint()
	if err != nil {
		return err
	}
	if ep.String() != advertised.String() {
		return fmt.Errorf("endpoint = %s, want %s", ep, advertised)
	}
	_, err = fmt.Fprintf(d.Out, "options: connection, endpoint, timeout, nats options\nserver-timeout: true\nendpoint: %s\nreply: %s\n", ep, reply.Message)
	return err
}

func main() { demo.Main(run) }
