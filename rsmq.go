package rsmq

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

type RedisSMQ struct {
	pool                        *redis.Pool
	config                      *RedisSMQConfig
	popMessageSha1              string
	receiveMessageSha1          string
	changeMessageVisibilitySha1 string
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

func NewRedisSMQ(config *RedisSMQConfig) (rsmq *RedisSMQ, err error) {
	rsmq = &RedisSMQ{
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
	defer func() {
		rec := recover()
		if rec != nil {
			switch tRec := rec.(type) {
			case error:
				err = tRec
			default:
				err = errors.New("unknown error")
			}
		}
		if rsmq != nil {
			_ = rsmq.quit()
		}
	}()
	conn := rsmq.pool.Get()
	if connErr := conn.Err(); connErr != nil {
		panic(connErr)
	}
	rsmq.popMessageSha1 = loadScript(conn, scriptPopMessage)
	rsmq.receiveMessageSha1 = loadScript(conn, scriptReceiveMessage)
	rsmq.changeMessageVisibilitySha1 = loadScript(conn, scriptChangeMessageVisibility)
	return
}

func NewDefaultRedisSMQ() (*RedisSMQ, error) {
	return NewRedisSMQ(NewDefaultRedisSMQConfig())
}

func NewDefaultRedisSMQConfig() *RedisSMQConfig {
	return &RedisSMQConfig{
		host:    "localhost",
		port:    6379,
		timeout: 5 * time.Second,
		ns:      "rsmq",
		ssl:     false,
	}
}

func (rsmq *RedisSMQ) quit() error {
	if rsmq.pool != nil {
		return rsmq.pool.Close()
	}
	return errors.New("redis pool is nil")
}

func redisAddress(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func loadScript(conn redis.Conn, script string) string {
	s := redis.NewScript(0, script)
	err := s.Load(conn)
	if err != nil {
		panic(err)
	}
	return s.Hash()
}
