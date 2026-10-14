package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

func run(ctx context.Context, d *demo.Demo) error {
	received := make(chan string, 4)
	// 两个实例订阅同一个服务；Publish 会广播给两个实例。
	for _, worker := range []string{"worker-1", "worker-2"} {
		srv := nats.NewServer(nats.Address(d.URL))
		_, err := api.RegisterGreeterNRServer(srv, &demo.Greeter{
			Notification: func(_ context.Context, req *api.NotifyRequest) (*api.Empty, error) {
				received <- worker + ": " + req.Content
				return &api.Empty{}, nil
			},
		})
		if err != nil {
			return err
		}
		if err := d.Start(ctx, srv); err != nil {
			return err
		}
	}
	client, err := nats.Dial(ctx, nats.WithAddress(d.URL))
	if err != nil {
		return err
	}
	d.Cleanup(client.Close)
	// Notify 返回 nil 只表示发送成功；下面的 channel 是演示程序的观测点，
	// 不是 Publish 的远程确认或响应。
	if err := api.NewGreeterNRClient(client).Notify(&api.NotifyRequest{Content: "published"}); err != nil {
		return err
	}
	messages := make([]string, 0, 2)
	for range 2 {
		msg, err := demo.Receive(ctx, received)
		if err != nil {
			return err
		}
		messages = append(messages, msg)
	}
	sort.Strings(messages) // 广播的处理顺序不确定。
	_, err = fmt.Fprintf(d.Out, "%s\n%s\n", messages[0], messages[1])
	return err
}

func main() { demo.Main(run) }
