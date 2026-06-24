package nats

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/byebyebruce/natsrpc"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
)

func TestTransport_Kind(t *testing.T) {
	tr := &Transport{}
	if tr.Kind() != transport.KindNATS {
		t.Errorf("Transport.Kind() = %v, want %v", tr.Kind(), transport.KindNATS)
	}
}

func TestTransport_Endpoint(t *testing.T) {
	tr := &Transport{endpoint: "nats://localhost:4222"}
	if tr.Endpoint() != "nats://localhost:4222" {
		t.Errorf("Transport.Endpoint() = %v, want %v", tr.Endpoint(), "nats://localhost:4222")
	}
}

func TestSubjectEndpoint(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{name: "namespace service id", parts: []string{"myapp", "helloworld.Greeter", "instance-1"}, want: "myapp.helloworld.Greeter.instance-1"},
		{name: "empty id is skipped", parts: []string{"myapp", "helloworld.Greeter", ""}, want: "myapp.helloworld.Greeter"},
		{name: "empty namespace is skipped", parts: []string{"", "helloworld.Greeter", "instance-1"}, want: "helloworld.Greeter.instance-1"},
		{name: "service only", parts: []string{"", "helloworld.Greeter", ""}, want: "helloworld.Greeter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := subjectEndpoint(tt.parts...); got != tt.want {
				t.Errorf("subjectEndpoint(%v) = %v, want %v", tt.parts, got, tt.want)
			}
		})
	}
}

func TestTransport_Operation(t *testing.T) {
	tr := &Transport{operation: "/helloworld.Greeter/SayHello"}
	if tr.Operation() != "/helloworld.Greeter/SayHello" {
		t.Errorf("Transport.Operation() = %v, want %v", tr.Operation(), "/helloworld.Greeter/SayHello")
	}
}

func TestHeaderCarrier(t *testing.T) {
	hc := make(headerCarrier)

	// Test Set and Get
	hc.Set("key1", "value1")
	if hc.Get("key1") != "value1" {
		t.Errorf("headerCarrier.Get() = %v, want %v", hc.Get("key1"), "value1")
	}

	// Test Add
	hc.Add("key2", "value2")
	if hc.Get("key2") != "value2" {
		t.Errorf("headerCarrier.Get() = %v, want %v", hc.Get("key2"), "value2")
	}

	// Test Keys
	keys := hc.Keys()
	if len(keys) != 2 {
		t.Errorf("headerCarrier.Keys() length = %v, want %v", len(keys), 2)
	}

	// Test Values
	values := hc.Values("key1")
	if len(values) != 1 || values[0] != "value1" {
		t.Errorf("headerCarrier.Values() = %v, want %v", values, []string{"value1"})
	}

	// Test Values for non-existent key
	values = hc.Values("nonexistent")
	if values != nil {
		t.Errorf("headerCarrier.Values() for nonexistent key = %v, want nil", values)
	}
}

func TestServer_Endpoint(t *testing.T) {
	tests := []struct {
		name      string
		address   string
		namespace string
		endpoint  *url.URL
		want      string
	}{
		{
			name:    "default address",
			address: "nats://localhost:4222",
			want:    "nats://localhost:4222",
		},
		{
			name:      "with namespace",
			address:   "nats://localhost:4222",
			namespace: "myapp",
			want:      "nats://localhost:4222?namespace=myapp",
		},
		{
			name:     "custom endpoint",
			address:  "nats://localhost:4222",
			endpoint: &url.URL{Scheme: "nats", Host: "custom:4222"},
			want:     "nats://custom:4222",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := NewServer(
				Address(tt.address),
				Namespace(tt.namespace),
			)
			if tt.endpoint != nil {
				srv.endpoint = tt.endpoint
			}

			ep, err := srv.Endpoint()
			if err != nil {
				t.Errorf("Server.Endpoint() error = %v", err)
				return
			}
			if ep.String() != tt.want {
				t.Errorf("Server.Endpoint() = %v, want %v", ep.String(), tt.want)
			}
		})
	}
}

func TestNewServer(t *testing.T) {
	srv := NewServer(
		Address("nats://localhost:4222"),
		Timeout(10*time.Second),
		Namespace("test"),
	)

	if srv.address != "nats://localhost:4222" {
		t.Errorf("Server.address = %v, want %v", srv.address, "nats://localhost:4222")
	}
	if srv.timeout != 10*time.Second {
		t.Errorf("Server.timeout = %v, want %v", srv.timeout, 10*time.Second)
	}
	if srv.namespace != "test" {
		t.Errorf("Server.namespace = %v, want %v", srv.namespace, "test")
	}
}

func TestNewRegistrar(t *testing.T) {
	srv := NewServer(Namespace("testns"))
	ref, err := NewRegistrar(srv, ServiceID("instance-1")).Register(natsrpc.ServiceDesc{ServiceName: "test.Service"}, nil)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got, want := ref.Name(), "testns.test.Service.instance-1"; got != want {
		t.Errorf("service name = %q, want %q", got, want)
	}
}

func TestServer_Use(t *testing.T) {
	srv := NewServer()

	mw := func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			return next(ctx, req)
		}
	}

	srv.Use("/*", mw)

	// Verify middleware is added
	matched := srv.middleware.Match("/test/method")
	if len(matched) != 1 {
		t.Errorf("Server.middleware.Match() length = %v, want %v", len(matched), 1)
	}
}
