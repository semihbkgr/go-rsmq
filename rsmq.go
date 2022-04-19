package rsmq

import (
	"fmt"
	"github.com/go-redis/redis"
	"strings"
)

const (
	q         = ":Q"
	queues    = "QUEUES"
	defaultNs = "rsmq"
)

var (
	hashPopMessage              = redis.NewScript(scriptPopMessage).Hash()
	hashReceiveMessage          = redis.NewScript(scriptReceiveMessage).Hash()
	hashChangeMessageVisibility = redis.NewScript(scriptChangeMessageVisibility).Hash()
)

// RedisSMQ client
type RedisSMQ struct {
	client *redis.Client
	ns     string
}

// NewRedisSMQ return new client
func NewRedisSMQ(client *redis.Client, ns string) *RedisSMQ {
	if ns == "" {
		ns = defaultNs
	}
	if !strings.HasSuffix(ns, ":") {
		ns += ":"
	}

	rsmq := &RedisSMQ{
		client: client,
		ns:     ns,
	}

	client.ScriptLoad(scriptPopMessage)
	client.ScriptLoad(scriptReceiveMessage)
	client.ScriptLoad(scriptChangeMessageVisibility)

	return rsmq
}

// CreateQueue creates a new queue
func (rsmq *RedisSMQ) CreateQueue(qname string, vt uint64, delay uint64, maxsize int64) error {
	if err := validateQname(qname); err != nil {
		return err
	}
	if err := validateVt(vt); err != nil {
		return err
	}
	if err := validateDelay(delay); err != nil {
		return err
	}
	if err := validateMaxsize(maxsize); err != nil {
		return err
	}

	time, err := rsmq.client.Time().Result()
	if err != nil {
		return err
	}

	key := rsmq.ns + qname + q

	tx := rsmq.client.TxPipeline()
	r := tx.HSetNX(key, "vt", vt)
	tx.HSetNX(key, "delay", delay)
	tx.HSetNX(key, "maxsize", maxsize)
	tx.HSetNX(key, "created", time.Unix())
	tx.HSetNX(key, "modified", time.Unix())
	if _, err = tx.Exec(); err != nil {
		return err
	}
	if !r.Val() {
		return fmt.Errorf("queue already exists: %s", qname)
	}

	_, err = rsmq.client.SAdd(rsmq.ns+queues, qname).Result()
	return err
}

// ListQueues lists queues
func (rsmq *RedisSMQ) ListQueues() ([]string, error) {
	return rsmq.client.SMembers(rsmq.ns + queues).Result()
}

// DeleteQueue delete queue
func (rsmq *RedisSMQ) DeleteQueue(qname string) error {
	if err := validateQname(qname); err != nil {
		return err
	}

	key := rsmq.ns + qname

	tx := rsmq.client.TxPipeline()
	r := tx.Del(key + q)
	tx.Del(key)
	tx.SRem(rsmq.ns+queues, qname)
	if _, err := tx.Exec(); err != nil {
		return nil
	}
	if r.Val() == 0 {
		return fmt.Errorf("queue not found: %s", qname)
	}

	return nil
}
