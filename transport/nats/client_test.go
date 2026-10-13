package nats

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

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
