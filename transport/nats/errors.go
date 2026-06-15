package nats

import (
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/go-kratos/kratos/v2/errors"
)

//todo: 莫名其妙

// EncodeError 把一个 error 序列化成字符串，通过 natsrpc 的 error header 回传
// 给调用方。Kratos 错误会被编码为其 Status（code/reason/message/metadata）的
// protojson，使完整的错误模型能够在往返过程中得以保留。非 Kratos 错误会先
// 通过 errors.FromError 转换。
//
// 如果 marshal 因任何原因失败，则回退为普通的 error 文本。
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

// DecodeError 在客户端侧对 EncodeError 进行逆向操作。由 EncodeError 产生的
// 字符串会被解析回带有完整 code/reason/message/metadata 的 *errors.Error。
// 不是合法 Status JSON 的字符串（例如来自 transport 的原始 "nats: timeout"）
// 会回退为携带原始文本的 unknown error。
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
