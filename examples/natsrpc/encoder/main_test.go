package main

import (
	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
	"testing"
)

func TestEncoder(t *testing.T) {
	demotest.Run(t, run, "Hello, JSON!\n")
}
