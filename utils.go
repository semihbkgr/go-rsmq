package rsmq

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
)

type signed interface {
	int | int64
}

type unsigned interface {
	uint | uint64
}

func toSigned[num signed](v any) (num, error) {
	switch val := v.(type) {
	case int:
		return num(val), nil
	case int64:
		return num(val), nil
	case string:
		r, err := strconv.ParseInt(val, 10, 0)
		return num(r), err
	default:
		err := errors.New("incompatible type to convert to signed")
		return 0, err
	}
}

func toUnsigned[num unsigned](v any) (num, error) {
	switch val := v.(type) {
	case uint:
		return num(val), nil
	case uint64:
		return num(val), nil
	case int:
		return num(val), nil
	case int64:
		return num(val), nil
	case string:
		r, err := strconv.ParseUint(val, 10, 0)
		return num(r), err
	default:
		err := errors.New("incompatible type to convert to unsigned")
		return 0, err
	}
}

func toString(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	default:
		err := errors.New("incompatible type to convert to string")
		return "", err
	}
}

func toUnsignedOrDef[num unsigned](v any, def num) num {
	n, err := toUnsigned[num](v)
	if err != nil {
		return def
	}
	return n
}

const idLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func makeID(l int) string {
	if l <= 0 {
		return ""
	}
	sb := strings.Builder{}
	length := len(idLetters)
	for i := 0; i < l; i++ {
		sb.WriteByte(idLetters[rand.Int()%length])
	}
	return sb.String()
}
