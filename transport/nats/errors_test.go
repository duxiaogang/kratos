package nats

import (
	"testing"

	kratoserrors "github.com/go-kratos/kratos/v2/errors"
)

func TestEncodeDecodeError_RoundTrip(t *testing.T) {
	orig := kratoserrors.BadRequest("MY_REASON", "bad input").
		WithMetadata(map[string]string{"k": "v"})

	got := kratoserrors.FromError(DecodeError(EncodeError(orig)))
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

func TestDecodeError_NonJSONFallback(t *testing.T) {
	// A raw transport error (e.g. nats timeout) is not Status JSON.
	err := DecodeError("nats: timeout")
	ke := kratoserrors.FromError(err)
	if int(ke.Code) != kratoserrors.UnknownCode {
		t.Errorf("code = %d, want %d", ke.Code, kratoserrors.UnknownCode)
	}
	if ke.Message != "nats: timeout" {
		t.Errorf("message = %q, want %q", ke.Message, "nats: timeout")
	}
}

func TestEncodeError_Nil(t *testing.T) {
	if got := EncodeError(nil); got != "" {
		t.Errorf("EncodeError(nil) = %q, want empty", got)
	}
}

func TestDecodeError_Empty(t *testing.T) {
	if got := DecodeError(""); got != nil {
		t.Errorf("DecodeError(\"\") = %v, want nil", got)
	}
}
