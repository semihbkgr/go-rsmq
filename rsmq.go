package rsmq

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

type RedisSMQ struct {
	pool   *redis.Pool
	config *RedisSMQConfig
}

func NewRedisSMQ(config *RedisSMQConfig) *RedisSMQ {
	return &RedisSMQ{
		config: config,
		pool: &redis.Pool{
			MaxIdle:   128,
			MaxActive: 128,
			Dial: func() (redis.Conn, error) {
				address := redisAddress(config.host, config.port)
				timeout := redis.DialConnectTimeout(config.timeout)
				password := redis.DialPassword(config.password)
				return redis.Dial("tcp", address, timeout, password)
			},
		},
	}
}

func NewDefaultRedisSMQ() *RedisSMQ {
	return NewRedisSMQ(NewDefaultRedisSMQConfig())
}

type RedisSMQConfig struct {
	host     string
	port     int
	timeout  time.Duration
	database string
	ns       string
	password string
	redisns  string
	ssl      bool
}

func NewDefaultRedisSMQConfig() *RedisSMQConfig {
	return &RedisSMQConfig{
		host:    "localhost",
		port:    6379,
		timeout: 5000,
		ns:      "rsmq",
		ssl:     false,
	}
}

func redisAddress(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}
