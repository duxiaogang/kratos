package nats

import (
	"testing"

	kratoserrors "github.com/go-kratos/kratos/v2/errors"
)

func TestEncodeDecodeError_RoundTrip(t *testing.T) {
	orig := kratoserrors.BadRequest("MY_REASON", "bad input").
		WithMetadata(map[string]string{"k": "v"})

	err, ok := decodeEncodedError(encodeError(orig))
	if !ok {
		t.Fatal("decodeEncodedError returned ok=false for an encoded error")
	}
	got := kratoserrors.FromError(err)
	if got.Code != orig.Code {
		t.Errorf("code = %d, want %d", got.Code, orig.Code)
	}
	if got.Reason != orig.Reason {
		t.Errorf("reason = %q, want %q", got.Reason, orig.Reason)
	}
	if got.Message != orig.Message {
		t.Errorf("message = %q, want %q", got.Message, orig.Message)
	}
	if got.Metadata["k"] != "v" {
		t.Errorf("metadata[k] = %q, want %q", got.Metadata["k"], "v")
	}
}

func TestDecodeEncodedError_NonJSON(t *testing.T) {
	// 原始的 transport 错误（例如 nats 超时）不是 Status JSON。
	if err, ok := decodeEncodedError("nats: timeout"); err != nil || ok {
		t.Errorf("decodeEncodedError(non-JSON) = (%v, %t), want (nil, false)", err, ok)
	}
}

func TestEncodeError_Nil(t *testing.T) {
	if got := encodeError(nil); got != "" {
		t.Errorf("encodeError(nil) = %q, want empty", got)
	}
}

func TestDecodeEncodedError_Empty(t *testing.T) {
	if err, ok := decodeEncodedError(""); err != nil || ok {
		t.Errorf("decodeEncodedError(empty) = (%v, %t), want (nil, false)", err, ok)
	}
}
