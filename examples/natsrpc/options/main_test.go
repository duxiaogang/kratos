package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestOptions(t *testing.T) {
	const want = "options: connection, endpoint, timeout, nats options\n" +
		"server-timeout: true\nendpoint: nats://advertised.example:4222\nreply: Hello, options!\n"
	demotest.Run(t, run, want)
}
