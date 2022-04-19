package rsmq

import (
	"errors"
	"strconv"
)

func strToInt(v any) (int, error) {
	switch val := v.(type) {
	case string:
		r, err := strconv.ParseInt(val, 10, 0)
		return int(r), err
	default:
		err := errors.New("incompatible type to convert to int")
		return 0, err
	}
}

func strToIntOrDef(v any, def int) (int, error) {
	if v == nil {
		return def, nil
	}
	return strToInt(v)
}

func strToUint(v any) (uint, error) {
	switch val := v.(type) {
	case string:
		r, err := strconv.ParseUint(val, 10, 0)
		return uint(r), err
	default:
		err := errors.New("incompatible type to convert to uint")
		return 0, err
	}
}

func strToUintOrDef(v any, def uint) (uint, error) {
	if v == nil {
		return def, nil
	}
	return strToUint(v)
}

func strToInt64(v any) (int64, error) {
	switch val := v.(type) {
	case string:
		r, err := strconv.ParseInt(val, 10, 0)
		return r, err
	default:
		err := errors.New("incompatible type to convert to int64")
		return 0, err
	}
}

func strToInt64OrDef(v any, def int64) (int64, error) {
	if v == nil {
		return def, nil
	}
	return strToInt64(v)
}

func strToUint64(v any) (uint64, error) {
	switch val := v.(type) {
	case string:
		r, err := strconv.ParseUint(val, 10, 0)
		return r, err
	default:
		err := errors.New("incompatible type to convert to uint64")
		return 0, err
	}
}

func strToUint64OrDef(v any, def uint64) (uint64, error) {
	if v == nil {
		return def, nil
	}
	return strToUint64(v)
}
