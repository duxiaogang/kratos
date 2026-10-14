package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestRequest(t *testing.T) {
	demotest.Run(t, run, "request: Hello, request!\n")
}
