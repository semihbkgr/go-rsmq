package rsmq

import (
	"github.com/go-redis/redis"
)

var hashPopMessage = redis.NewScript(scriptPopMessage).Hash()
var hashReceiveMessage = redis.NewScript(scriptReceiveMessage).Hash()
var hashChangeMessageVisibility = redis.NewScript(scriptChangeMessageVisibility).Hash()

// RedisSMQ client
type RedisSMQ struct {
	client    *redis.Client
	namespace string
}

type queueDescriptor struct {
	qname   string
	vt      int
	delay   int
	maxSize int
	ts      int64
	uid     string
}

// NewRedisSMQ return new client
func NewRedisSMQ(client *redis.Client, namespace string) *RedisSMQ {
	verifyNamespace(&namespace)

	rsmq := &RedisSMQ{
		client:    client,
		namespace: namespace,
	}

	client.ScriptLoad(scriptPopMessage)
	client.ScriptLoad(scriptReceiveMessage)
	client.ScriptLoad(scriptChangeMessageVisibility)

	return rsmq
}
