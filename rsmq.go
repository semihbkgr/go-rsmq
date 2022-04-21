package rsmq

import (
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// unset values
const (
	UnsetVt      = ^uint(0)
	UnsetDelay   = ^uint(0)
	UnsetMaxsize = -(int(^uint(0)>>1) - 1)
)

const (
	q      = ":Q"
	queues = "QUEUES"
)

const (
	defaultNs      = "rsmq"
	defaultVt      = 30
	defaultDelay   = 0
	defaultMaxsize = 65536
)

// Queue and message errors
var (
	ErrQueueNotFound   = errors.New("queue not found")
	ErrQueueExists     = errors.New("queue exists")
	ErrMessageTooLong  = errors.New("message too long")
	ErrMessageNotFound = errors.New("message not found")
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

// QueueAttributes queue attributes
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

// QueueMessage queue message
type QueueMessage struct {
	Id      string
	Message string
	Rc      uint64
	Fr      time.Time
	Sent    time.Time
}

// NewRedisSMQ return new client
func NewRedisSMQ(client *redis.Client, ns string) *RedisSMQ {
	if client == nil {
		panic("nil redis client")
	}

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
	if vt == UnsetVt {
		vt = defaultVt
	}
	if delay == UnsetDelay {
		delay = defaultDelay
	}
	if maxsize == UnsetMaxsize {
		maxsize = defaultMaxsize
	}

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

	queue, err := rsmq.getQueue(qname, false)
	if err != nil {
		return nil, err
	}

	key := rsmq.ns + qname

	tx := rsmq.client.TxPipeline()
	hmGetSliceCmd := tx.HMGet(key+q, "vt", "delay", "maxsize", "totalrecv", "totalsent", "created", "modified")
	zCardIntCmd := tx.ZCard(key)
	zCountIntCmd := tx.ZCount(key, strconv.FormatInt(int64(queue.ts), 10), "+inf")
	if _, err := tx.Exec(); err != nil {
		return nil, err
	}

	hmGetValues := hmGetSliceCmd.Val()

	vt, err := toUnsigned[uint](hmGetValues[0])
	if err != nil {
		return nil, errors.Wrapf(err, "visibility timeout: %v", hmGetValues[0])
	}
	delay, err := toUnsigned[uint](hmGetValues[1])
	if err != nil {
		return nil, errors.Wrapf(err, "delay: %v", hmGetValues[1])
	}
	maxsize, err := toSigned[int](hmGetValues[2])
	if err != nil {
		return nil, errors.Wrapf(err, "max size: %v", hmGetValues[2])
	}
	totalRecv := toUnsignedOrDef[uint64](hmGetValues[3], 0)

	totalSent := toUnsignedOrDef[uint64](hmGetValues[4], 0)

	created, err := toUnsigned[uint64](hmGetValues[5])
	if err != nil {
		return nil, errors.Wrapf(err, "created: %v", hmGetValues[5])
	}
	modified, err := toUnsigned[uint64](hmGetValues[6])
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

	queue, err := rsmq.getQueue(qname, false)
	if err != nil {
		return nil, err
	}

	if vt == UnsetVt {
		vt = queue.vt
	}
	if delay == UnsetDelay {
		delay = queue.delay
	}
	if maxsize == UnsetMaxsize {
		maxsize = queue.maxsize
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
		return nil, ErrQueueNotFound
	}
	vt, err := toUnsigned[uint](hmGetValues[0])
	if err != nil {
		return nil, errors.Wrapf(err, "visibility timeout: %v", hmGetValues[0])
	}
	delay, err := toUnsigned[uint](hmGetValues[1])
	if err != nil {
		return nil, errors.Wrapf(err, "delay: %v", hmGetValues[1])
	}
	maxsize, err := toSigned[int](hmGetValues[2])
	if err != nil {
		return nil, errors.Wrapf(err, "max size: %v", hmGetValues[2])
	}

	t := timeCmd.Val()

	randUID := ""
	if uid {
		randUID = strconv.FormatInt(t.UnixNano()/1000, 36) + makeID(22)
	}

	return &queueDef{
		vt:      vt,
		delay:   delay,
		maxsize: maxsize,
		ts:      uint64(t.UnixMilli()),
		uid:     randUID,
	}, nil
}

// SendMessage send message
func (rsmq *RedisSMQ) SendMessage(qname string, message string, delay uint) (string, error) {
	if err := validateQname(qname); err != nil {
		return "", err
	}

	queue, err := rsmq.getQueue(qname, true)
	if err != nil {
		return "", err
	}

	if delay == UnsetDelay {
		delay = queue.delay
	}

	if err := validateDelay(delay); err != nil {
		return "", err
	}

	if queue.maxsize != -1 && len(message) > queue.maxsize {
		return "", ErrMessageTooLong
	}

	key := rsmq.ns + qname

	tx := rsmq.client.TxPipeline()
	tx.ZAdd(key, redis.Z{
		Score:  float64(queue.ts + uint64(delay)*1000),
		Member: queue.uid,
	})
	tx.HSet(key+q, queue.uid, message)
	tx.HIncrBy(key+q, "totalsent", 1)
	if _, err := tx.Exec(); err != nil {
		return "", err
	}

	return queue.uid, nil
}

// ReceiveMessage receive message
func (rsmq *RedisSMQ) ReceiveMessage(qname string, vt uint) (*QueueMessage, error) {
	if err := validateQname(qname); err != nil {
		return nil, err
	}

	queue, err := rsmq.getQueue(qname, true)
	if err != nil {
		return nil, err
	}

	if vt == UnsetVt {
		vt = queue.vt
	}

	if err := validateVt(vt); err != nil {
		return nil, err
	}

	key := rsmq.ns + qname

	qvt := strconv.FormatUint(queue.ts+uint64(vt)*1000, 10)
	ct := strconv.FormatUint(queue.ts, 10)

	evalCmd := rsmq.client.EvalSha(hashReceiveMessage, []string{key, ct, qvt})
	return rsmq.createQueueMessage(evalCmd)
}

// PopMessage pop message
func (rsmq *RedisSMQ) PopMessage(qname string) (*QueueMessage, error) {
	if err := validateQname(qname); err != nil {
		return nil, err
	}

	queue, err := rsmq.getQueue(qname, false)
	if err != nil {
		return nil, err
	}

	key := rsmq.ns + qname

	t := strconv.FormatUint(queue.ts, 10)

	evalCmd := rsmq.client.EvalSha(hashPopMessage, []string{key, t})
	return rsmq.createQueueMessage(evalCmd)
}

func (rsmq *RedisSMQ) createQueueMessage(cmd *redis.Cmd) (*QueueMessage, error) {
	vals, ok := cmd.Val().([]any)
	if !ok {
		return nil, errors.New("mismatched message response type")
	}
	if len(vals) == 0 {
		return nil, nil
	}
	id, err := toString(vals[0])
	if err != nil {
		return nil, errors.Wrapf(err, "id: %v", vals[0])
	}
	message, err := toString(vals[1])
	if err != nil {
		return nil, errors.Wrapf(err, "message: %v", vals[1])
	}
	rc, err := toUnsigned[uint64](vals[2])
	if err != nil {
		return nil, errors.Wrapf(err, "received count: %v", vals[2])
	}
	fr, err := toSigned[int64](vals[3])
	if err != nil {
		return nil, errors.Wrapf(err, "first received time: %v", vals[3])
	}
	sent, err := strconv.ParseInt(id[0:10], 36, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "sent time: %v", vals[4])
	}

	return &QueueMessage{
		Id:      id,
		Message: message,
		Rc:      rc,
		Fr:      time.UnixMilli(fr),
		Sent:    time.UnixMilli(sent),
	}, nil
}

// ChangeMessageVisibility changes message visibility
func (rsmq *RedisSMQ) ChangeMessageVisibility(qname string, id string, vt uint) error {
	if err := validateQname(qname); err != nil {
		return err
	}
	if err := validateID(id); err != nil {
		return err
	}

	queue, err := rsmq.getQueue(qname, false)
	if err != nil {
		return err
	}

	key := rsmq.ns + qname
	t := strconv.FormatUint(queue.ts+uint64(vt)*1000, 10)

	evalCmd := rsmq.client.EvalSha(hashChangeMessageVisibility, []string{key, id, t})
	if e, err := evalCmd.Bool(); err != nil {
		return err
	} else if !e {
		return ErrMessageNotFound
	}

	return nil
}

// DeleteMessage delete message
func (rsmq *RedisSMQ) DeleteMessage(qname string, id string) error {
	if err := validateQname(qname); err != nil {
		return err
	}
	if err := validateID(id); err != nil {
		return err
	}

	_, err := rsmq.getQueue(qname, false)
	if err != nil {
		return err
	}

	key := rsmq.ns + qname

	tx := rsmq.client.TxPipeline()
	zremIntCmd := tx.ZRem(key, id)
	hdelIntCmd := tx.HDel(key+q, id, id+":rc", id+":fr")
	if _, err := tx.Exec(); err != nil {
		return err
	}

	if zremIntCmd.Val() != 1 || hdelIntCmd.Val() == 0 {
		return ErrMessageNotFound
	}

	return nil
}
