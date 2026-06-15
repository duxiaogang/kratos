package nats

import (
	"github.com/go-kratos/kratos/v2/transport"
)

var _ transport.Transporter = (*Transport)(nil)

// Transport 是 NATS transport。
type Transport struct {
	endpoint    string //namespace.service.id
	operation   string //method
	reqHeader   headerCarrier
	replyHeader headerCarrier
}

// Kind 返回 transport 的类型。
func (tr *Transport) Kind() transport.Kind {
	return transport.KindNATS
}

// Endpoint 返回 transport 的 endpoint。
func (tr *Transport) Endpoint() string {
	return tr.endpoint
}

// Operation 返回 transport 的 operation。
func (tr *Transport) Operation() string {
	return tr.operation
}

// RequestHeader 返回请求 header。
func (tr *Transport) RequestHeader() transport.Header {
	return tr.reqHeader
}

// ReplyHeader 返回响应 header。
func (tr *Transport) ReplyHeader() transport.Header {
	return tr.replyHeader
}

// headerCarrier 是一个 NATS header 载体。
type headerCarrier map[string]string

// Get 返回与所传 key 关联的值。
func (hc headerCarrier) Get(key string) string {
	return hc[key]
}

// Set 存储 key-value 键值对。
func (hc headerCarrier) Set(key string, value string) {
	hc[key] = value
}

// Add 向 key-value 键值对追加值。
func (hc headerCarrier) Add(key string, value string) {
	hc[key] = value
}

// Keys 列出该载体中存储的所有 key。
func (hc headerCarrier) Keys() []string {
	keys := make([]string, 0, len(hc))
	for k := range hc {
		keys = append(keys, k)
	}
	return keys
}

// Values 返回与所传 key 关联的值的切片。
func (hc headerCarrier) Values(key string) []string {
	if v, ok := hc[key]; ok {
		return []string{v}
	}
	return nil
}
