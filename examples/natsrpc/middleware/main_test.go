package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestMiddleware(t *testing.T) {
	const want = "client before (request-header=client) -> server before (request-header=client) -> " +
		"handler -> server after (reply-header=server) -> client after (reply-header=server)\n"
	demotest.Run(t, run, want)
}
