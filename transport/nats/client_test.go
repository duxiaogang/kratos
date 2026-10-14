package nats

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	kratoserrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/nats-io/nats.go"
)

type blockingDialer struct {
	started chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (d *blockingDialer) Dial(string, string) (net.Conn, error) {
	close(d.started)
	<-d.release
	close(d.done)
	return nil, errors.New("dialer released")
}

func TestDialCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Dial(ctx, WithAddress("nats://127.0.0.1:4222"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial() error = %v, want %v", err, context.Canceled)
	}
}

func TestDialContextCancelsConnectionAttempt(t *testing.T) {
	dialer := &blockingDialer{
		started: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := Dial(ctx,
			WithAddress("nats://127.0.0.1:4222"),
			WithNatsOptions(nats.SetCustomDialer(dialer)),
		)
		errCh <- err
	}()

	select {
	case <-dialer.started:
	case <-time.After(time.Second):
		t.Fatal("NATS connection attempt did not start")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Dial() error = %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("Dial() did not return after context deadline")
	}

	close(dialer.release)
	select {
	case <-dialer.done:
	case <-time.After(time.Second):
		t.Fatal("NATS connection attempt did not finish after release")
	}
}

func TestDecodeRemoteError(t *testing.T) {
	remote := kratoserrors.BadRequest("BAD_INPUT", "invalid input").WithMetadata(map[string]string{"field": "name"})
	got := decodeRemoteError(errors.New(encodeError(remote)))
	decoded := kratoserrors.FromError(got)
	if decoded.Code != remote.Code || decoded.Reason != remote.Reason || decoded.Message != remote.Message {
		t.Fatalf("decoded remote error = %v, want %v", got, remote)
	}
	if decoded.Metadata["field"] != "name" {
		t.Fatalf("decoded remote metadata = %v, want field=name", decoded.Metadata)
	}

	for _, local := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		kratoserrors.ServiceUnavailable("LOCAL", "local failure"),
		errors.New("nats: timeout"),
	} {
		if got := decodeRemoteError(local); got != local {
			t.Errorf("decodeRemoteError(%v) = %v, want original error", local, got)
		}
	}
}

func TestClientMiddlewareErrorPreserved(t *testing.T) {
	want := kratoserrors.BadRequest("LOCAL", "middleware failure")
	client := &Client{
		middleware: []middleware.Middleware{
			func(next middleware.Handler) middleware.Handler {
				return func(context.Context, any) (any, error) {
					return nil, want
				}
			},
		},
	}

	if got := client.Request(context.Background(), "service", "method", nil, nil); got != want {
		t.Fatalf("client middleware error = %v, want original %v", got, want)
	}
}

func TestClientRequestDeadline(t *testing.T) {
	tests := []struct {
		name              string
		callerTimeout     time.Duration
		clientTimeout     time.Duration
		wantClientTimeout bool
	}{
		{name: "no caller deadline", clientTimeout: time.Minute, wantClientTimeout: true},
		{name: "longer caller deadline", callerTimeout: time.Hour, clientTimeout: time.Minute, wantClientTimeout: true},
		{name: "shorter caller deadline", callerTimeout: time.Minute, clientTimeout: time.Hour},
		{name: "zero timeout with caller deadline", callerTimeout: time.Minute},
		{name: "zero timeout without caller deadline"},
		{name: "negative timeout with caller deadline", callerTimeout: time.Minute, clientTimeout: -time.Second},
		{name: "negative timeout without caller deadline", clientTimeout: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.callerTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.callerTimeout)
				defer cancel()
			}
			var callCtx context.Context
			client := &Client{
				timeout: tt.clientTimeout,
				middleware: []middleware.Middleware{
					func(middleware.Handler) middleware.Handler {
						return func(ctx context.Context, _ any) (any, error) {
							callCtx = ctx
							return nil, nil
						}
					},
				},
			}

			before := time.Now()
			if err := client.Request(ctx, "service", "method", nil, nil); err != nil {
				t.Fatalf("Request() error = %v", err)
			}
			after := time.Now()
			deadline, ok := callCtx.Deadline()
			if tt.wantClientTimeout {
				if !ok || deadline.Before(before.Add(tt.clientTimeout)) || deadline.After(after.Add(tt.clientTimeout)) {
					t.Fatalf("request deadline = %v, has deadline = %v, want client timeout %v", deadline, ok, tt.clientTimeout)
				}
			} else if want, wantOK := ctx.Deadline(); ok != wantOK || !deadline.Equal(want) {
				t.Fatalf("request deadline = %v, %v, want caller deadline %v, %v", deadline, ok, want, wantOK)
			}
		})
	}
}
