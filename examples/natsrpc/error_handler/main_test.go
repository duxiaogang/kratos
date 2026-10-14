package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestErrorHandler(t *testing.T) {
	demotest.Run(t, run, "publish returned nil\nerror handler: 400 INVALID_NOTIFICATION\n")
}
