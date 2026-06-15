package nats

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/byebyebruce/natsrpc"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/wrapperspb"

	kratoserrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

const (
	testServiceName = "kratos.nats.test.Echo"
	testMethod      = "Echo"
	testErrReason   = "ECHO_BAD_INPUT"
)

// echoServer 是被测的 natsrpc handler。
type echoServer struct{}

func echoHandler(svc interface{}, ctx context.Context, req interface{}) (interface{}, error) {
	in := req.(*wrapperspb.StringValue)
	if in.GetValue() == "boom" {
		return nil, kratoserrors.BadRequest(testErrReason, "value must not be boom").
			WithMetadata(map[string]string{"field": "value"})
	}
	// 原样回显，并把收到的 header 透出，以便测试断言其是否被正确传播。
	out := in.GetValue()
	if tr, ok := transport.FromServerContext(ctx); ok {
		if v := tr.RequestHeader().Get("x-echo"); v != "" {
			out = out + "|" + v
		}
	}
	return &wrapperspb.StringValue{Value: out}, nil
}

var echoServiceDesc = natsrpc.ServiceDesc{
	ServiceName: testServiceName,
	Methods: []natsrpc.MethodDesc{
		{
			MethodName:  testMethod,
			Handler:     echoHandler,
			RequestType: reflect.TypeOf(wrapperspb.StringValue{}),
			IsPublish:   false,
		},
	},
	Metadata: "echo.proto",
}

// dialTestConn 在没有可达的 NATS 服务端时跳过测试。
func dialTestConn(t *testing.T) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(nats.DefaultURL, nats.Timeout(500*time.Millisecond))
	if err != nil {
		t.Skipf("no NATS server at %s: %v", nats.DefaultURL, err)
	}
	return conn
}

// startTestServer 装配一个共享给定连接的 Server 并运行它。
func startTestServer(t *testing.T, conn *nats.Conn, opts ...ServerOption) *Server {
	t.Helper()
	opts = append([]ServerOption{Connection(conn)}, opts...)
	srv := NewServer(opts...)
	if _, err := srv.Register(echoServiceDesc, &echoServer{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	go func() {
		if err := srv.Start(context.Background()); err != nil {
			t.Errorf("Start: %v", err)
		}
	}()
	// 在客户端发送请求前，给 Start 一点时间完成订阅。
	waitFor(t, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.started
	})
	return srv
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}

func TestIntegration_RequestReply(t *testing.T) {
	conn := dialTestConn(t)
	defer conn.Close()
	srv := startTestServer(t, conn)
	defer srv.Stop(context.Background())

	cli, err := Dial(context.Background(), WithConnection(conn))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	rep := &wrapperspb.StringValue{}
	if err := cli.Request(context.Background(), testServiceName, testMethod,
		&wrapperspb.StringValue{Value: "hello"}, rep); err != nil {
		t.Fatalf("Request: %v", err)
	}
	if rep.GetValue() != "hello" {
		t.Errorf("reply = %q, want %q", rep.GetValue(), "hello")
	}
}

func TestIntegration_BusinessErrorRoundTrip(t *testing.T) {
	conn := dialTestConn(t)
	defer conn.Close()
	srv := startTestServer(t, conn)
	defer srv.Stop(context.Background())

	cli, err := Dial(context.Background(), WithConnection(conn))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	rep := &wrapperspb.StringValue{}
	err = cli.Request(context.Background(), testServiceName, testMethod,
		&wrapperspb.StringValue{Value: "boom"}, rep)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	ke := kratoserrors.FromError(err)
	if got := ke.Reason; got != testErrReason {
		t.Errorf("reason = %q, want %q", got, testErrReason)
	}
	if got := int(ke.Code); got != 400 {
		t.Errorf("code = %d, want 400", got)
	}
	if got := ke.Metadata["field"]; got != "value" {
		t.Errorf("metadata[field] = %q, want %q", got, "value")
	}
}

func TestIntegration_Middleware(t *testing.T) {
	conn := dialTestConn(t)
	defer conn.Close()

	var serverSawOp string
	mw := func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			if tr, ok := transport.FromServerContext(ctx); ok {
				serverSawOp = tr.Operation()
			}
			return next(ctx, req)
		}
	}
	srv := startTestServer(t, conn, Middleware(mw))
	defer srv.Stop(context.Background())

	cli, err := Dial(context.Background(), WithConnection(conn))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	rep := &wrapperspb.StringValue{}
	if err := cli.Request(context.Background(), testServiceName, testMethod,
		&wrapperspb.StringValue{Value: "x"}, rep); err != nil {
		t.Fatalf("Request: %v", err)
	}
	want := "/" + testServiceName + "/" + testMethod
	if serverSawOp != want {
		t.Errorf("server middleware saw operation %q, want %q", serverSawOp, want)
	}
}

func TestIntegration_HeaderPropagation(t *testing.T) {
	conn := dialTestConn(t)
	defer conn.Close()
	srv := startTestServer(t, conn)
	defer srv.Stop(context.Background())

	// 客户端中间件向 transport 写入一个请求 header。
	clientMW := func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			if tr, ok := transport.FromClientContext(ctx); ok {
				tr.RequestHeader().Set("x-echo", "tag")
			}
			return next(ctx, req)
		}
	}
	cli, err := Dial(context.Background(), WithConnection(conn), WithMiddleware(clientMW))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	rep := &wrapperspb.StringValue{}
	if err := cli.Request(context.Background(), testServiceName, testMethod,
		&wrapperspb.StringValue{Value: "v"}, rep); err != nil {
		t.Fatalf("Request: %v", err)
	}
	if rep.GetValue() != "v|tag" {
		t.Errorf("reply = %q, want %q (header not propagated)", rep.GetValue(), "v|tag")
	}
}
