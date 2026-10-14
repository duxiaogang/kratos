// Package demotest runs the same code as each example against a real broker.
package demotest

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demo"
)

func Run(t *testing.T, run demo.Run, want string) {
	t.Helper()
	if testing.Short() {
		t.Skip("NATS examples require a real broker")
	}
	address := broker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var out bytes.Buffer
	d := &demo.Demo{URL: address, Out: &out}
	// Also clean up if an example panics or a future assertion calls Fatal.
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	err := run(ctx, d)
	closeErr := d.Close()
	if err != nil {
		t.Fatalf("example: %v\noutput:\n%s", err, &out)
	}
	if closeErr != nil {
		t.Fatalf("cleanup: %v", closeErr)
	}
	if got := out.String(); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
}

// Each test starts its own broker on an OS-assigned loopback port. With an
// external NATS_URL, run go test -p 1 to avoid subject collisions.
func broker(t *testing.T) string {
	t.Helper()
	if address := os.Getenv("NATS_URL"); address != "" {
		return address
	}
	binary, err := exec.LookPath("nats-server")
	if err != nil {
		if os.Getenv("NATS_REQUIRED") == "1" {
			t.Fatal("install nats-server or set NATS_URL; NATS_REQUIRED=1 forbids skipping")
		}
		t.Skip("install nats-server or set NATS_URL (use NATS_REQUIRED=1 to require integration tests)")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if os.Getenv("NATS_REQUIRED") == "1" {
			t.Fatal(fmt.Errorf("cannot allocate a local test port: %w", err))
		}
		t.Skipf("cannot allocate a local test port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	cmd := exec.Command(binary, "-a", "127.0.0.1", "-p", strconv.Itoa(port))
	var log bytes.Buffer
	cmd.Stdout = &log
	cmd.Stderr = &log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start nats-server: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("nats-server did not exit")
		}
		if t.Failed() {
			t.Logf("broker log:\n%s", log.String())
		}
	})
	address := "nats://127.0.0.1:" + strconv.Itoa(port)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		conn, err := nats.Connect(address, nats.Timeout(100*time.Millisecond), nats.NoReconnect())
		if err == nil {
			conn.Close()
			return address
		}
		select {
		case err := <-done:
			done <- err
			if os.Getenv("NATS_REQUIRED") == "1" {
				t.Fatal(fmt.Errorf("nats-server exited: %v\n%s", err, log.String()))
			}
			t.Skipf("nats-server is unavailable: %v", err)
		case <-ctx.Done():
			t.Fatal("nats-server did not become ready")
		case <-ticker.C:
		}
	}
}
