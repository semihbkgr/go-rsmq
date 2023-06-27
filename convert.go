package rsmq

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

func convertToSigned[n int | int64](a any) n {
	switch v := a.(type) {
	case int:
		return n(v)
	case int64:
		return n(v)
	case string:
		r, err := strconv.ParseInt(v, 10, 0)
		if err != nil {
			panic(err)
		}
		return n(r)
	default:
		panic(fmt.Errorf("type: %T, incompatible type to convert to signed", a))
	}
}

func convertToUnsigned[n uint | uint64](a any) n {
	switch v := a.(type) {
	case uint:
		return n(v)
	case uint64:
		return n(v)
	case int:
		return n(v)
	case int64:
		return n(v)
	case string:
		r, err := strconv.ParseUint(v, 10, 0)
		if err != nil {
			panic(err)
		}
		return n(r)
	default:
		panic(fmt.Errorf("type: %T, incompatible type to convert to unsigned", a))
	}
}

func convertStringToUnsigned[n uint | uint64](s any, def n) n {
	if s == nil {
		return def
	}
	r, err := strconv.ParseUint(s.(string), 10, 0)
	if err != nil {
		panic(err)
	}
	return n(r)
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
