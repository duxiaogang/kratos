package main

import (
	"context"
	"os"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/nats"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
)

func main() {
	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
	)
	log.SetLogger(logger)

	// Create NATS client.
	// The standard protobuf encoder is the default, so no WithEncoder() option is needed.
	client, err := nats.Dial(context.Background(),
		nats.WithEndpoint("nats://localhost:4222"),
		nats.WithNamespace("example"),
	)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer client.Close()

	// Create greeter client
	greeter := api.NewGreeterNRClient(client)

	// Test SayHello RPC
	log.Info("Calling SayHello...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := greeter.SayHello(ctx, &api.HelloRequest{
		Name: "Kratos",
	})
	if err != nil {
		log.Fatalf("SayHello failed: %v", err)
	}
	log.Infof("SayHello response: %s", reply.Message)

	// Test Notify (publish, no response)
	log.Info("Calling Notify...")
	err = greeter.Notify(&api.NotifyRequest{
		Content: "Hello from client!",
	})
	if err != nil {
		log.Fatalf("Notify failed: %v", err)
	}
	log.Info("Notify sent successfully")

	// Wait a bit to ensure the notification is processed
	time.Sleep(500 * time.Millisecond)

	log.Info("Client finished")
}
