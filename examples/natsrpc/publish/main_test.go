package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestPublish(t *testing.T) {
	demotest.Run(t, run, "worker-1: published\nworker-2: published\n")
}
