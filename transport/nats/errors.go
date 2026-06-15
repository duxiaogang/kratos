package nats

import (
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/go-kratos/kratos/v2/errors"
)

//todo: 莫名其妙

// EncodeError serializes an error into a string carried back to the caller via
// the natsrpc error header. Kratos errors are encoded as protojson of their
// Status (code/reason/message/metadata) so the full error model survives the
// round trip. Non-Kratos errors are converted via errors.FromError first.
//
// If marshaling fails for any reason, it falls back to the plain error text.
func EncodeError(err error) string {
	if err == nil {
		return ""
	}
	se := errors.FromError(err)
	b, mErr := protojson.Marshal(&se.Status)
	if mErr != nil {
		return err.Error()
	}
	return string(b)
}

// DecodeError reverses EncodeError on the client side. A string produced by
// EncodeError is parsed back into a *errors.Error with code/reason/message/
// metadata intact. Strings that are not valid Status JSON (e.g. a raw
// "nats: timeout" from the transport) fall back to an unknown error carrying
// the original text.
func DecodeError(s string) error {
	if s == "" {
		return nil
	}
	st := &errors.Status{}
	if err := protojson.Unmarshal([]byte(s), st); err != nil || st.Code == 0 {
		return errors.New(errors.UnknownCode, errors.UnknownReason, s)
	}
	e := errors.New(int(st.Code), st.Reason, st.Message)
	if len(st.Metadata) > 0 {
		e = e.WithMetadata(st.Metadata)
	}
	return e
}
