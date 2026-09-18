package metrics

import (
	"go.opentelemetry.io/otel/attribute"

	rxpcore "github.com/relexec/rxp/core"
	apikindversion "github.com/relexec/rxp/kindversion"
)

const (
	AttributeNameErrCode = "error.code"
)

type hasStatusCode interface {
	StatusCode() int
}

// AttributeErrCode returns the error code attribute KeyValue with the value of
// the supplied error's code.
func AttributeErrCode(err error) attribute.KeyValue {
	code := 500
	if hc, ok := err.(hasStatusCode); ok {
		code = hc.StatusCode()
	}
	return attribute.Int(AttributeNameErrCode, code)
}

const (
	AttributeNameType = "type"
)

// AttributeType returns the target type attribute KeyValue with the
// value of the supplied target type.
func AttributeType(tt rxpcore.Type) attribute.KeyValue {
	return attribute.String(AttributeNameType, string(tt))
}

const (
	AttributeNameKindVersion = "kindversion"
)

// AttributeKindVersion returns the kindversion attribute KeyValue with the
// value of the supplied KindVersionName.
func AttributeKindVersion(kv apikindversion.Name) attribute.KeyValue {
	return attribute.String(AttributeNameKindVersion, string(kv))
}
