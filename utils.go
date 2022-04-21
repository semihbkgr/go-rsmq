package rsmq

import (
	"fmt"
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

func toSigned[num signed](val any) (num, error) {
	switch v := val.(type) {
	case int:
		return num(v), nil
	case int64:
		return num(v), nil
	case string:
		r, err := strconv.ParseInt(v, 10, 0)
		return num(r), err
	default:
		err := fmt.Errorf("type: %T, incompatible type to convert to signed", val)
		return 0, err
	}
}

func toUnsigned[num unsigned](val any) (num, error) {
	switch v := val.(type) {
	case uint:
		return num(v), nil
	case uint64:
		return num(v), nil
	case int:
		return num(v), nil
	case int64:
		return num(v), nil
	case string:
		r, err := strconv.ParseUint(v, 10, 0)
		return num(r), err
	default:
		err := fmt.Errorf("type: %T, incompatible type to convert to unsigned", val)
		return 0, err
	}
}

func toString(val any) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	default:
		err := fmt.Errorf("type: %T, incompatible type to convert to string", val)
		return "", err
	}
}

func toUnsignedOrDef[num unsigned](val any, def num) num {
	n, err := toUnsigned[num](val)
	if err != nil {
		return def
	}
	return n
}

const idLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func makeID(n int) string {
	if n <= 0 {
		return ""
	}
	sb := strings.Builder{}
	ln := len(idLetters)
	for i := 0; i < n; i++ {
		sb.WriteByte(idLetters[rand.Int()%ln])
	}
	return sb.String()
}
