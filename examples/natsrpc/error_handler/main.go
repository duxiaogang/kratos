package main

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

func run(ctx context.Context, d *demo.Demo) error {
	handled := make(chan any, 1)
	srv := nats.NewServer(nats.Address(d.URL), nats.ErrorHandler(func(err any) { handled <- err }))
	_, err := api.RegisterGreeterNRServer(srv, &demo.Greeter{Notification: func(context.Context, *api.NotifyRequest) (*api.Empty, error) {
		return nil, errors.BadRequest("INVALID_NOTIFICATION", "content rejected")
	}})
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
	// Publish 不会把业务错误返回客户端，服务端可通过 ErrorHandler 上报。
	if err := api.NewGreeterNRClient(client).Notify(&api.NotifyRequest{Content: "bad"}); err != nil {
		return err
	}
	reported, err := demo.Receive(ctx, handled)
	if err != nil {
		return err
	}
	var status errors.Status
	if err := protojson.Unmarshal([]byte(fmt.Sprint(reported)), &status); err != nil {
		return err
	}
	if status.Code != 400 || status.Reason != "INVALID_NOTIFICATION" || status.Message != "content rejected" {
		return fmt.Errorf("unexpected reported error: %v", reported)
	}
	fmt.Fprintf(d.Out, "publish returned nil\nerror handler: %d %s\n", status.Code, status.Reason)
	return nil
}

func main() { demo.Main(run) }
