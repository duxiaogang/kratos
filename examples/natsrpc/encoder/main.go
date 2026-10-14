package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/byebyebruce/natsrpc"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
	"github.com/go-kratos/kratos/v2/transport/nats"
)

// 普通 Go struct 不能使用默认的 ProtoEncoder。两端约定相同的 JSON 编码。
type helloRequest struct {
	Name string `json:"name"`
}
type helloReply struct {
	Message string `json:"message"`
}
type jsonEncoder struct{}

func (jsonEncoder) Encode(v any) ([]byte, error)    { return json.Marshal(v) }
func (jsonEncoder) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }

func run(ctx context.Context, d *demo.Demo) error {
	srv := nats.NewServer(nats.Address(d.URL), nats.Encoder(jsonEncoder{}))
	_, err := srv.Register(natsrpc.ServiceDesc{
		ServiceName: "example.JSON",
		Methods: []natsrpc.MethodDesc{{
			MethodName:     "Hello",
			RequestFactory: func() any { return &helloRequest{} },
			Handler: func(_ any, _ context.Context, req any) (any, error) {
				return &helloReply{Message: "Hello, " + req.(*helloRequest).Name + "!"}, nil
			},
		}},
	}, nil)
	if err != nil {
		return err
	}
	if err := d.Start(ctx, srv, nats.WithEncoder(jsonEncoder{})); err != nil {
		return err
	}
	client, err := nats.Dial(ctx, nats.WithAddress(d.URL), nats.WithEncoder(jsonEncoder{}))
	if err != nil {
		return err
	}
	d.Cleanup(client.Close)
	var reply helloReply
	if err := client.Request(ctx, "example.JSON", "Hello", &helloRequest{Name: "JSON"}, &reply); err != nil {
		return err
	}
	fmt.Fprintln(d.Out, reply.Message)
	return nil
}

func main() { demo.Main(run) }
