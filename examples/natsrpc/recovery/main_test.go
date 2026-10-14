package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestRecovery(t *testing.T) {
	demotest.Run(t, run, "publish panic: recovered\nrequest panic: client deadline exceeded\nHello, still running!\n")
}
