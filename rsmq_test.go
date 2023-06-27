package rsmq

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"

	"github.com/go-redis/redis"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type redisContainer struct {
	testcontainers.Container
	address string
}

func setupRedis(ctx context.Context) (*redisContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "redis:6.2.6-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("* Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	mappedPort, err := container.MappedPort(ctx, "6379")
	if err != nil {
		return nil, err
	}

	hostIP, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	address := fmt.Sprintf("%s:%d", hostIP, mappedPort.Int())

	return &redisContainer{Container: container, address: address}, nil
}

var redisCnt *redisContainer
var ctx context.Context

func setup() error {
	ctx = context.Background()
	rc, err := setupRedis(ctx)
	redisCnt = rc

	return err
}

func shutdown() error {
	if redisCnt != nil {
		return redisCnt.Container.Terminate(ctx)
	}

	return nil
}

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		log.Print(err)
	}

	code := m.Run()

	err = shutdown()
	if err != nil {
		log.Print(err)
	}

	os.Exit(code)
}

func preIntegrationTest(t *testing.T) *redis.Client {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	if redisCnt == nil || ctx == nil {
		t.Error("env cannot be set correctly")
		t.FailNow()
	}

	client := redis.NewClient(&redis.Options{
		Addr: redisCnt.address,
	})

	t.Cleanup(func() {
		client.FlushAll()
		client.ScriptFlush()
		client.FlushDB()
	})

	return client
}

func TestNewRedisSMQ(t *testing.T) {
	client := preIntegrationTest(t)

	ns := "test"
	rsmq := NewRedisSMQ(client, ns)

	assert.NotNil(t, rsmq, "rsmq is nil")
	assert.NotNil(t, rsmq.client, "clint in rsmq is nil")

	assert.Equal(t, ns+":", rsmq.ns, "namespace is not as expected")

	assert.Panics(t, func() {
		NewRedisSMQ(nil, ns)
	}, "not panicking when redis client is not")
}

func TestRedisSMQ_CreateQueue(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	err = rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.NotNil(t, err, "error is nil on creating the existing queue")
	assert.Equal(t, ErrQueueExists, err, "error is not as expected")

	err = rsmq.CreateQueue("it is invalid queue name", UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.NotNil(t, err, "error is nil on creating a queue with invalid name")
}

func TestRedisSMQ_ListQueues(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname1 := "que1"
	qname2 := "que2"
	qname3 := "que3"

	err := rsmq.CreateQueue(qname1, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	err = rsmq.CreateQueue(qname2, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	err = rsmq.CreateQueue(qname3, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	queues, err := rsmq.ListQueues()
	assert.Nil(t, err, "error is not nil on listing queues")
	assert.Len(t, queues, 3, "queues length is not as expected")
	assert.Contains(t, queues, qname1)
	assert.Contains(t, queues, qname2)
	assert.Contains(t, queues, qname3)
}

func TestRedisSMQ_GetQueueAttributes(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	queAttrib, err := rsmq.GetQueueAttributes(qname)
	assert.Nil(t, err, "error is not nil on getting queue attributes")
	assert.NotNil(t, queAttrib, "queueAttributes is nil")

	queAttrib, err = rsmq.GetQueueAttributes("non-existing")
	assert.NotNil(t, err, "error is nil on getting attributes of non-existing queue")
	assert.Equal(t, ErrQueueNotFound, err, "error is as expected")
	assert.Nil(t, queAttrib, "queueAttributes is not nil on getting attributes of non-existing queue")

	queAttrib, err = rsmq.GetQueueAttributes("it is invalid queue name")
	assert.NotNil(t, err, "error is nil on getting attributes of queue with invalid name")
	assert.Equal(t, ErrInvalidQname, err, "error is as expected")
	assert.Nil(t, queAttrib, "queueAttributes is not nil on getting attributes of queue with invalid name")
}

func TestRedisSMQ_SetQueueAttributes(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	newVt := uint(30)
	newDelay := uint(60)
	newMaxsize := 2048

	queAttrib, err := rsmq.SetQueueAttributes(qname, newVt, newDelay, newMaxsize)
	assert.Nil(t, err, "error is not nil on setting queue attributes")
	assert.NotNil(t, queAttrib, "queueAttributes is nil")

	queAttrib, err = rsmq.GetQueueAttributes(qname)
	assert.Nil(t, err, "error is not nil on getting queue attributes")
	assert.NotNil(t, queAttrib, "queueAttributes is nil")
	assert.Equal(t, newVt, queAttrib.Vt, "queueAttributes Vt is not as expected")
	assert.Equal(t, newDelay, queAttrib.Delay, "queueAttributes Delay is not as expected")
	assert.Equal(t, newMaxsize, queAttrib.Maxsize, "queueAttributes Maxsize is not as expected")

	queAttrib, err = rsmq.SetQueueAttributes("non-existing", UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.NotNil(t, err, "error is nil on setting attributes of non-existing queue")
	assert.Equal(t, ErrQueueNotFound, err, "error is as expected")
	assert.Nil(t, queAttrib, "queueAttributes is not nil on setting attributes of non-existing queue")

	queAttrib, err = rsmq.SetQueueAttributes("it is invalid queue name", UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.NotNil(t, err, "error is nil on setting attributes of queue with invalid name")
	assert.Equal(t, ErrInvalidQname, err, "error is as expected")
	assert.Nil(t, queAttrib, "queueAttributes is not nil on setting attributes of queue with invalid name")
}

func TestRedisSMQ_Quit(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	err := rsmq.Quit()
	assert.Nil(t, err, "error is not nil on quit")
}

func TestRedisSMQ_DeleteQueue(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	err = rsmq.DeleteQueue(qname)
	assert.Nil(t, err, "error is not nil on deleting the queue")

	queues, err := rsmq.ListQueues()
	assert.Nil(t, err, "error is not nil on listing queues")
	assert.Empty(t, queues, "queue slice is not empty")

	err = rsmq.DeleteQueue("non-existing")
	assert.NotNil(t, err, "error is nil on deleting non-existing queue")
	assert.Equal(t, ErrQueueNotFound, err, "error is as expected")

	err = rsmq.DeleteQueue("it is invalid queue name")
	assert.NotNil(t, err, "error is nil on deleting queue with invalid name")
	assert.Equal(t, ErrInvalidQname, err, "error is as expected")
}

func TestRedisSMQ_SendMessage(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname1 := "que1"

	err := rsmq.CreateQueue(qname1, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	message := "message"

	id, err := rsmq.SendMessage(qname1, message, UnsetDelay)
	assert.Nil(t, err, "error is not nil on sending a message")
	assert.NotEmpty(t, id, "id is empty on sending a message")

	qname2 := "que2"
	err = rsmq.CreateQueue(qname2, UnsetVt, UnsetDelay, 1024)
	assert.Nil(t, err, "error is not nil on creating a queue")

	b := make([]byte, 2048)
	message = string(b)
	id, err = rsmq.SendMessage(qname2, message, UnsetDelay)
	assert.NotNil(t, err, "error is nil on sending a message which exceeds the size limit")
	assert.Equal(t, ErrMessageTooLong, err, "error is as expected")
	assert.Empty(t, id, "id is not empty on sending a message which exceeds the size limit")

	id, err = rsmq.SendMessage("non-existing", message, UnsetDelay)
	assert.NotNil(t, err, "error is nil on sending a message to non-existing queue")
	assert.Equal(t, ErrQueueNotFound, err, "error is as expected")
	assert.Empty(t, id, "id is not empty on sending a message to non-existing queue")

	id, err = rsmq.SendMessage("it is invalid queue name", message, UnsetDelay)
	assert.NotNil(t, err, "error is nil on sending a message to the queue with invalid name")
	assert.Equal(t, ErrInvalidQname, err, "error is as expected")
	assert.Empty(t, id, "id is not empty on sending a message to the queue with invalid name")
}

func TestRedisSMQ_ReceiveMessage(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	message := "message"
	id, err := rsmq.SendMessage(qname, message, UnsetDelay)
	assert.Nil(t, err, "error is not nil on sending a message")
	assert.NotEmpty(t, id, "id is empty on sending a message")

	queMsg, err := rsmq.ReceiveMessage(qname, UnsetVt)
	assert.Nil(t, err, "error is not nil on receiving the message")
	assert.NotNil(t, queMsg, "queueMessage is nil")
	assert.Equal(t, id, queMsg.ID, "queueMessage ID is not as expected")
	assert.Equal(t, message, queMsg.Message, "queueMessage Message is not as expected")

	queMsg, err = rsmq.ReceiveMessage(qname, UnsetVt)
	assert.Nil(t, err, "error is not nil on receiving a message from empty queue")
	assert.Nil(t, queMsg, "queueMessage is not nil on receiving a message from empty queue")

	queMsg, err = rsmq.ReceiveMessage("non-existing", UnsetVt)
	assert.NotNil(t, err, "error is nil on receiving the message from non-existing queue")
	assert.Equal(t, ErrQueueNotFound, err, "error is as expected")
	assert.Empty(t, queMsg, "queueMessage is not empty on receiving the message from non-existing queue")

	queMsg, err = rsmq.ReceiveMessage("it is invalid queue name", UnsetVt)
	assert.NotNil(t, err, "error is nil on receiving the message from the queue with invalid name")
	assert.Equal(t, ErrInvalidQname, err, "error is as expected")
	assert.Empty(t, queMsg, "queueMessage is not empty on receiving the message from the queue with invalid name")
}

func TestRedisSMQ_PopMessage(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	message := "message"
	id, err := rsmq.SendMessage(qname, message, UnsetDelay)
	assert.Nil(t, err, "error is not nil on sending a message")
	assert.NotEmpty(t, id, "id is empty on sending a message")

	queMsg, err := rsmq.PopMessage(qname)
	assert.Nil(t, err, "error is not nil on pop the message")
	assert.NotNil(t, queMsg, "queueMessage is nil")
	assert.Equal(t, id, queMsg.ID, "queueMessage ID is not as expected")
	assert.Equal(t, message, queMsg.Message, "queueMessage Message is not as expected")

	queMsg, err = rsmq.PopMessage(qname)
	assert.Nil(t, err, "error is not nil on pop a message from empty queue")
	assert.Nil(t, queMsg, "queueMessage is not nil on pop a message from empty queue")

	queMsg, err = rsmq.PopMessage("non-existing")
	assert.NotNil(t, err, "error is nil on pop the message from non-existing queue")
	assert.Equal(t, ErrQueueNotFound, err, "error is as expected")
	assert.Empty(t, queMsg, "queueMessage is not empty on pop the message from non-existing queue")

	queMsg, err = rsmq.PopMessage("it is invalid queue name")
	assert.NotNil(t, err, "error is nil on pop the message from the queue with invalid name")
	assert.Equal(t, ErrInvalidQname, err, "error is as expected")
	assert.Empty(t, queMsg, "queueMessage is not empty on pop the message from the queue with invalid name")
}

func TestRedisSMQ_ChangeMessageVisibility(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	message := "message"
	id, err := rsmq.SendMessage(qname, message, UnsetDelay)
	assert.Nil(t, err, "error is not nil on sending a message")
	assert.NotEmpty(t, id, "id is empty on sending a message")

	newVt := uint(0)
	err = rsmq.ChangeMessageVisibility(qname, id, newVt)
	assert.Nil(t, err, "error is not nil on changing the message visibility")

	queMsg, err := rsmq.PopMessage(qname)
	assert.Nil(t, err, "error is not nil on pop the message")
	assert.NotNil(t, queMsg, "queueMessage is nil")
	assert.Equal(t, id, queMsg.ID, "queueMessage ID is not as expected")
	assert.Equal(t, message, queMsg.Message, "queueMessage Message is not as expected")

	err = rsmq.ChangeMessageVisibility(qname, id, UnsetVt)
	assert.NotNil(t, err, "error is nil on changing the visibility of message of non-existing message")
	assert.Equal(t, ErrMessageNotFound, err, "error is not as expected")

	err = rsmq.ChangeMessageVisibility("non-existing", id, UnsetVt)
	assert.NotNil(t, err, "error is nil on changing the visibility of a message in non-existing queue")
	assert.Equal(t, ErrQueueNotFound, err, "error is as expected")

	err = rsmq.ChangeMessageVisibility("it is invalid queue name", id, UnsetVt)
	assert.NotNil(t, err, "error is nil on changing the visibility of a message in the queue with invalid name")
	assert.Equal(t, ErrInvalidQname, err, "error is as expected")
}

func TestRedisSMQ_DeleteMessage(t *testing.T) {
	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")
	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)
	assert.Nil(t, err, "error is not nil on creating a queue")

	message := "message"
	id, err := rsmq.SendMessage(qname, message, UnsetVt)
	assert.Nil(t, err, "error is not nil on sending a message")
	assert.NotEmpty(t, id, "id is empty on sending a message")

	err = rsmq.DeleteMessage(qname, id)
	assert.Nil(t, err, "error is not nil on deleting the message")

	queMsg, err := rsmq.ReceiveMessage(qname, UnsetVt)
	assert.Nil(t, err, "error is not nil on receiving a message from empty queue")
	assert.Nil(t, queMsg, "queueMessage is not nil on receiving a message from empty queue")

	err = rsmq.DeleteMessage(qname, id)
	assert.NotNil(t, err, "error is nil on deleting non-existing message")
	assert.Equal(t, ErrMessageNotFound, err, "error is not as expected")

	err = rsmq.DeleteMessage("non-existing", id)
	assert.NotNil(t, err, "error is nil on deleting a message in non-existing queue")
	assert.Equal(t, ErrQueueNotFound, err, "error is as expected")

	err = rsmq.DeleteMessage("it is invalid queue name", id)
	assert.NotNil(t, err, "error is nil on deleting a message in the queue with invalid name")
	assert.Equal(t, ErrInvalidQname, err, "error is as expected")
}
