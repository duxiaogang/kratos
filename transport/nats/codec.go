package nats

import (
	"github.com/byebyebruce/natsrpc"
	"google.golang.org/protobuf/proto"
)

var _ natsrpc.Encoder = (*ProtoEncoder)(nil)

// ProtoEncoder is a standard protobuf encoder for natsrpc.
// Use this instead of the default gogo/protobuf encoder when
// using standard protobuf generated code.
type ProtoEncoder struct{}

// Encode encodes the message using standard protobuf.
func (ProtoEncoder) Encode(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return proto.Marshal(v.(proto.Message))
}

// Decode decodes the message using standard protobuf.
func (ProtoEncoder) Decode(data []byte, vPtr interface{}) error {
	if len(data) == 0 {
		return nil
	}
	return proto.Unmarshal(data, vPtr.(proto.Message))
}
