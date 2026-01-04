package nats

import (
	"github.com/go-kratos/kratos/v2/transport"
)

var _ transport.Transporter = (*Transport)(nil)

// Transport is a NATS transport.
type Transport struct {
	endpoint    string
	operation   string
	reqHeader   headerCarrier
	replyHeader headerCarrier
}

// Kind returns the transport kind.
func (tr *Transport) Kind() transport.Kind {
	return transport.KindNATS
}

// Endpoint returns the transport endpoint.
func (tr *Transport) Endpoint() string {
	return tr.endpoint
}

// Operation returns the transport operation.
func (tr *Transport) Operation() string {
	return tr.operation
}

// RequestHeader returns the request header.
func (tr *Transport) RequestHeader() transport.Header {
	return tr.reqHeader
}

// ReplyHeader returns the reply header.
func (tr *Transport) ReplyHeader() transport.Header {
	return tr.replyHeader
}

// headerCarrier is a NATS header carrier.
type headerCarrier map[string]string

// Get returns the value associated with the passed key.
func (hc headerCarrier) Get(key string) string {
	return hc[key]
}

// Set stores the key-value pair.
func (hc headerCarrier) Set(key string, value string) {
	hc[key] = value
}

// Add append value to key-values pair.
func (hc headerCarrier) Add(key string, value string) {
	hc[key] = value
}

// Keys lists the keys stored in this carrier.
func (hc headerCarrier) Keys() []string {
	keys := make([]string, 0, len(hc))
	for k := range hc {
		keys = append(keys, k)
	}
	return keys
}

// Values returns a slice of values associated with the passed key.
func (hc headerCarrier) Values(key string) []string {
	if v, ok := hc[key]; ok {
		return []string{v}
	}
	return nil
}
