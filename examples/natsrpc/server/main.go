package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport/nats"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/api"
)

// GreeterService implements api.GreeterNRServer
type GreeterService struct{}

// SayHello implements the SayHello RPC method
func (s *GreeterService) SayHello(ctx context.Context, req *api.HelloRequest) (*api.HelloReply, error) {
	log.Infof("Received SayHello request: name=%s", req.Name)
	return &api.HelloReply{
		Message: fmt.Sprintf("Hello, %s!", req.Name),
	}, nil
}

// Notify implements the Notify publish method
func (s *GreeterService) Notify(ctx context.Context, req *api.NotifyRequest) (*api.Empty, error) {
	log.Infof("Received Notify: content=%s", req.Content)
	return &api.Empty{}, nil
}

func main() {
	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
	)
	log.SetLogger(logger)

	// Create NATS server with middleware
	natsSrv := nats.NewServer(
		nats.Address("nats://localhost:4222"),
		nats.Namespace("example"),
		nats.Encoder(nats.ProtoEncoder{}), // Use standard protobuf encoder
		nats.Middleware(
			recovery.Recovery(),
			logging.Server(logger),
		),
	)

	// Register greeter service
	svc, err := api.RegisterGreeterNRServer(natsSrv, &GreeterService{})
	if err != nil {
		log.Fatalf("Failed to register service: %v", err)
	}
	defer svc.Close()

	// Create Kratos app
	app := kratos.New(
		kratos.Name("natsrpc-server"),
		kratos.Server(natsSrv),
	)

	// Handle shutdown signals
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		if err := app.Stop(); err != nil {
			log.Errorf("Failed to stop app: %v", err)
		}
	}()

	log.Info("Starting NATS RPC server...")
	if err := app.Run(); err != nil {
		log.Fatalf("Failed to run app: %v", err)
	}
}
