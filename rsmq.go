package rsmq

import (
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

const (
	q         = ":Q"
	queues    = "QUEUES"
	defaultNs = "rsmq"
)

var (
	ErrQueueNotFound = errors.New("queue not found")
	ErrQueueExists   = errors.New("queue exists")
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

type QueueAttributes struct {
	Vt         uint
	Delay      uint
	Maxsize    int
	TotalRecv  uint64
	TotalSent  uint64
	Created    uint64
	Modified   uint64
	Msgs       uint64
	HiddenMsgs uint64
}

type queueDef struct {
	vt      uint
	delay   uint
	maxsize int
	ts      uint64
	uid     string
	qname   string
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
func (rsmq *RedisSMQ) CreateQueue(qname string, vt uint, delay uint, maxsize int) error {
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

	t, err := rsmq.client.Time().Result()
	if err != nil {
		return err
	}

	key := rsmq.ns + qname + q

	tx := rsmq.client.TxPipeline()
	r := tx.HSetNX(key, "vt", vt)
	tx.HSetNX(key, "delay", delay)
	tx.HSetNX(key, "maxsize", maxsize)
	tx.HSetNX(key, "created", t.Unix())
	tx.HSetNX(key, "modified", t.Unix())
	if _, err = tx.Exec(); err != nil {
		return err
	}
	if !r.Val() {
		return ErrQueueExists
	}

	_, err = rsmq.client.SAdd(rsmq.ns+queues, qname).Result()
	return err
}

// ListQueues lists queues
func (rsmq *RedisSMQ) ListQueues() ([]string, error) {
	return rsmq.client.SMembers(rsmq.ns + queues).Result()
}

// GetQueueAttributes get queue attributes
func (rsmq *RedisSMQ) GetQueueAttributes(qname string) (*QueueAttributes, error) {
	if err := validateQname(qname); err != nil {
		return nil, err
	}

	t, err := rsmq.client.Time().Result()
	if err != nil {
		return nil, err
	}

	key := rsmq.ns + qname

	tx := rsmq.client.TxPipeline()
	hmGetSliceCmd := tx.HMGet(key+q, "vt", "delay", "maxsize", "totalrecv", "totalsent", "created", "modified")
	zCardIntCmd := tx.ZCard(key)
	zCountIntCmd := tx.ZCount(key, strconv.FormatInt(t.UnixMilli(), 10), "+inf")
	if _, err := tx.Exec(); err != nil {
		return nil, err
	}

	hmGetValues := hmGetSliceCmd.Val()

	vt, err := strToUint(hmGetValues[0])
	if err != nil {
		return nil, errors.Wrapf(err, "visibility timeout: %v", hmGetValues[0])
	}
	delay, err := strToUint(hmGetValues[1])
	if err != nil {
		return nil, errors.Wrapf(err, "delay: %v", hmGetValues[1])
	}
	maxsize, err := strToInt(hmGetValues[2])
	if err != nil {
		return nil, errors.Wrapf(err, "max size: %v", hmGetValues[2])
	}
	totalRecv, err := strToUint64OrDef(hmGetValues[3], 0)
	if err != nil {
		return nil, errors.Wrapf(err, "total retrieve: %v", hmGetValues[3])
	}
	totalSent, err := strToUint64OrDef(hmGetValues[4], 0)
	if err != nil {
		return nil, errors.Wrapf(err, "total sent: %v", hmGetValues[4])
	}
	created, err := strToUint64(hmGetValues[5])
	if err != nil {
		return nil, errors.Wrapf(err, "created: %v", hmGetValues[5])
	}
	modified, err := strToUint64(hmGetValues[6])
	if err != nil {
		return nil, errors.Wrapf(err, "modified: %v", hmGetValues[6])
	}
	msgs := uint64(zCardIntCmd.Val())
	hiddenMsgs := uint64(zCountIntCmd.Val())

	return &QueueAttributes{
		Vt:         vt,
		Delay:      delay,
		Maxsize:    maxsize,
		TotalRecv:  totalRecv,
		TotalSent:  totalSent,
		Created:    created,
		Modified:   modified,
		Msgs:       msgs,
		HiddenMsgs: hiddenMsgs,
	}, nil

}

// SetQueueAttributes set queue attributes
func (rsmq *RedisSMQ) SetQueueAttributes(qname string, vt uint, delay uint, maxsize int) (*QueueAttributes, error) {
	if err := validateQname(qname); err != nil {
		return nil, err
	}
	if err := validateVt(vt); err != nil {
		return nil, err
	}
	if err := validateDelay(delay); err != nil {
		return nil, err
	}
	if err := validateMaxsize(maxsize); err != nil {
		return nil, err
	}

	key := rsmq.ns + qname + q

	queue, err := rsmq.getQueue(qname, false)
	if err != nil {
		return nil, err
	}

	tx := rsmq.client.TxPipeline()
	tx.HSet(key, "modified", queue.ts)
	tx.HSet(key, "vt", vt)
	tx.HSet(key, "delay", delay)
	tx.HSet(key, "maxsize", maxsize)
	if _, err := tx.Exec(); err != nil {
		return nil, err
	}

	return rsmq.GetQueueAttributes(qname)
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
		return ErrQueueNotFound
	}

	return nil
}

func (rsmq *RedisSMQ) getQueue(qname string, uid bool) (*queueDef, error) {

	key := rsmq.ns + qname + q

	tx := rsmq.client.TxPipeline()

	hmGetSliceCmd := tx.HMGet(key, "vt", "delay", "maxsize")
	timeCmd := tx.Time()
	if _, err := tx.Exec(); err != nil {
		return nil, err
	}

	hmGetValues := hmGetSliceCmd.Val()
	if hmGetValues[0] == nil || hmGetValues[1] == nil || hmGetValues[2] == nil {
		return nil, errors.New("")
	}
	vt, err := strToUint(hmGetValues[0])
	if err != nil {
		return nil, errors.Wrapf(err, "visibility timeout: %v", hmGetValues[0])
	}
	delay, err := strToUint(hmGetValues[1])
	if err != nil {
		return nil, errors.Wrapf(err, "delay: %v", hmGetValues[1])
	}
	maxsize, err := strToInt(hmGetValues[2])
	if err != nil {
		return nil, errors.Wrapf(err, "max size: %v", hmGetValues[2])
	}

	t := timeCmd.Val()

	randUid := ""
	if uid {
		randUid = uuid.NewString()
	}

	return &queueDef{
		vt:      vt,
		delay:   delay,
		maxsize: maxsize,
		ts:      uint64(t.UnixMilli()),
		uid:     randUid,
	}, nil
}
