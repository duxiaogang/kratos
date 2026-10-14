package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestReplyLater(t *testing.T) {
	demotest.Run(t, run, "Hello, reply later!\n")
}
