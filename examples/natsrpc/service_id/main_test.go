package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestServiceId(t *testing.T) {
	demotest.Run(t, run, "request: greeter-1; publish: greeter-1: targeted\nrequest: greeter-2; publish: greeter-2: targeted\nempty/missing id: no responders\n")
}
