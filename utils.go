package rsmq

import (
	"errors"
	"strconv"
)

type signed interface {
	int | int64
}

type unsigned interface {
	uint | uint64
}

func toSigned[num signed](v any) (n num, err error) {
	switch val := v.(type) {
	case string:
		r, err := strconv.ParseInt(val, 10, 0)
		return num(r), err
	default:
		err := errors.New("incompatible type to convert")
		return 0, err
	}
}

func toUnsigned[num unsigned](v any) (n num, err error) {
	switch val := v.(type) {
	case string:
		r, err := strconv.ParseUint(val, 10, 0)
		return num(r), err
	default:
		err := errors.New("incompatible type to convert")
		return 0, err
	}
}

func toSignedOrDef[num signed](v any, def num) num {
	n, err := toSigned[num](v)
	if err != nil {
		return def
	}
	return n
}

func toUnsignedOrDef[num unsigned](v any, def num) num {
	n, err := toUnsigned[num](v)
	if err != nil {
		return def
	}
	return n
}
