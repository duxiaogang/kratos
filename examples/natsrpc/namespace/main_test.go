package main

import (
	"testing"

	"github.com/go-kratos/kratos/v2/examples/natsrpc/internal/demotest"
)

func TestNamespace(t *testing.T) {
	demotest.Run(t, run, "tenant-a: hello\ntenant-b: hello\nmissing namespace: no responders\n")
}
