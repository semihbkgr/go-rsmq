package rsmq

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
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

	if rsmq == nil {
		t.Fatal("rsmq is nil")
	}
	if rsmq.client == nil {
		t.Error("client is nil")
	}
	if !strings.HasPrefix(rsmq.ns, ns) {
		t.Error("ns is not as excepted")
	}

	NewRedisSMQ(client, ns)

	func() {
		defer func() {
			err := recover()
			if err == nil {
				t.Error("error is nil when redis client is nil")
			}
		}()
		NewRedisSMQ(nil, ns)
	}()

}

func TestRedisSMQ_CreateQueue(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	err = rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err == nil {
		t.Fatal("error is nil when creating queue with existing qname")
	}
	if err != ErrQueueExists {
		t.Error("error is not as expected")
	}

	err = rsmq.CreateQueue("it is not valid", UnsetVt, UnsetDelay, UnsetMaxsize)

	if err == nil {
		t.Fatal("error is nil when creating queue with invalid qname")
	}

}

func TestRedisSMQ_ListQueues(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname1 := "que1"
	qname2 := "que2"
	qname3 := "que3"

	err := rsmq.CreateQueue(qname1, UnsetVt, UnsetDelay, UnsetMaxsize)
	if err != nil {
		t.Fatal(err)
	}

	err = rsmq.CreateQueue(qname2, UnsetVt, UnsetDelay, UnsetMaxsize)
	if err != nil {
		t.Fatal(err)
	}

	err = rsmq.CreateQueue(qname3, UnsetVt, UnsetDelay, UnsetMaxsize)
	if err != nil {
		t.Fatal(err)
	}

	queues, err := rsmq.ListQueues()

	if err != nil {
		t.Fatal(err)
	}

	containsStr := func(strings []string, str string) bool {
		for _, s := range strings {
			if s == str {
				return true
			}
		}
		return false
	}

	if len(queues) != 3 {
		t.Error("length of queues is not as expected")
	}
	if !containsStr(queues, qname1) {
		t.Errorf("queues not contain %v", qname1)
	}
	if !containsStr(queues, qname2) {
		t.Errorf("queues not contain %v", qname2)
	}
	if !containsStr(queues, qname3) {
		t.Errorf("queues not contain %v", qname3)
	}

}

func TestRedisSMQ_GetQueueAttributes(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	queAttrib, err := rsmq.GetQueueAttributes(qname)

	if err != nil {
		t.Fatal(err)
	}
	if queAttrib == nil {
		t.Error("queue attributes is nil")
	}

	_, err = rsmq.GetQueueAttributes("non-existing")

	if err == nil {
		t.Error("error is nil when getting attribute of non-existing queue")
	}
	if err != ErrQueueNotFound {
		t.Error("error is not as expected")
	}

}

func TestRedisSMQ_SetQueueAttributes(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	newVt := uint(30)
	newDelay := uint(60)
	newMaxsize := 2048

	queAttrib, err := rsmq.SetQueueAttributes(qname, newVt, newDelay, newMaxsize)

	if err != nil {
		t.Fatal(err)
	}
	if queAttrib == nil {
		t.Error("queue attributes is nil")
	}

	queAttrib, err = rsmq.GetQueueAttributes(qname)

	if err != nil {
		t.Fatal(err)
	}
	if queAttrib == nil {
		t.Fatal("queue attributes is nil")
	}
	if queAttrib.Vt != newVt {
		t.Error("queue attributes vt is not expected")
	}
	if queAttrib.Delay != newDelay {
		t.Error("queue attributes delay is not expected")
	}
	if queAttrib.Maxsize != newMaxsize {
		t.Error("queue attributes maxsize is not expected")
	}

	_, err = rsmq.SetQueueAttributes("non-existing", UnsetVt, UnsetDelay, UnsetMaxsize)

	if err == nil {
		t.Error("error is nil when getting attribute of non-existing queue")
	}
	if err != ErrQueueNotFound {
		t.Error("error is not as expected")
	}

}

func TestRedisSMQ_Quit(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	err := rsmq.Quit()

	if err != nil {
		t.Fatal(err)
	}

}

func TestRedisSMQ_DeleteQueue(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	err = rsmq.DeleteQueue(qname)

	if err != nil {
		t.Fatal(err)
	}

	queues, err := rsmq.ListQueues()

	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 0 {
		t.Error("queue is not deleted")
	}

	err = rsmq.DeleteQueue("non-existing")

	if err == nil {
		t.Error("error is nil when deleting non-existing error")
	}
	if err != ErrQueueNotFound {
		t.Error("error is not as expected")
	}

}

func TestRedisSMQ_SendMessage(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname1 := "que1"

	err := rsmq.CreateQueue(qname1, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	message := "message"

	_, err = rsmq.SendMessage(qname1, message, UnsetDelay)

	if err != nil {
		t.Error(err)
	}

	qname2 := "que2"

	err = rsmq.CreateQueue(qname2, UnsetVt, UnsetDelay, 1024)

	if err != nil {
		t.Fatal(err)
	}

	b := make([]byte, 2048)
	for i := range b {
		b[i] = 'e'
	}

	_, err = rsmq.SendMessage(qname2, string(b), UnsetDelay)

	if err == nil {
		t.Error("error is nil when exceeding maxsize")
	}
	if err != ErrMessageTooLong {
		t.Error("error is not as expected")
	}

	_, err = rsmq.SendMessage("non-existing", message, UnsetDelay)

	if err == nil {
		t.Error("error is nil when sending message to non-existing queue")
	}
	if err != ErrQueueNotFound {
		t.Error("error is not as expected")
	}

}

func TestRedisSMQ_ReceiveMessage(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	message := "message"

	id, err := rsmq.SendMessage(qname, message, UnsetDelay)

	if err != nil {
		t.Fatal(err)
	}

	queMsg, err := rsmq.ReceiveMessage(qname, UnsetVt)

	if err != nil {
		t.Fatal(err)
	}

	if queMsg == nil {
		t.Fatal("queue message is nil")
	}
	if queMsg.ID != id {
		t.Error("id is not as expected")
	}
	if queMsg.Message != message {
		t.Error("message is not as expected")
	}

	queMsg, err = rsmq.ReceiveMessage(qname, UnsetVt)

	if err != nil {
		t.Error("error is not nil when receiving message from empty queue")
	}
	if queMsg != nil {
		t.Error("message is not nil when receiving message from empty queue")
	}

	_, err = rsmq.ReceiveMessage("non-existing", UnsetVt)

	if err == nil {
		t.Error("error is when receiving message from non-existing queue")
	}
	if err != ErrQueueNotFound {
		t.Error("error is not as expected")
	}

}

func TestRedisSMQ_PopMessage(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	message := "message"

	id, err := rsmq.SendMessage(qname, message, UnsetDelay)

	if err != nil {
		t.Fatal(err)
	}

	queMsg, err := rsmq.PopMessage(qname)

	if err != nil {
		t.Fatal(err)
	}

	if queMsg == nil {
		t.Fatal("queue message is nil")
	}
	if queMsg.ID != id {
		t.Error("id is not as expected")
	}
	if queMsg.Message != message {
		t.Error("message is not as expected")
	}

	queMsg, err = rsmq.PopMessage(qname)

	if err != nil {
		t.Error("error is not nil when pop message from empty queue")
	}
	if queMsg != nil {
		t.Error("message is not nil when pop message from empty queue")
	}

	_, err = rsmq.PopMessage("non-existing")

	if err == nil {
		t.Error("error is nil when pop message from non-existing queue")
	}
	if err != ErrQueueNotFound {
		t.Error("error is not as expected")
	}

}

func TestRedisSMQ_ChangeMessageVisibility(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	message := "message"

	id, err := rsmq.SendMessage(qname, message, UnsetDelay)

	if err != nil {
		t.Fatal(err)
	}

	newVt := uint(0)

	err = rsmq.ChangeMessageVisibility(qname, id, newVt)

	if err != nil {
		t.Error(err)
	}

	_, err = rsmq.PopMessage(qname)

	if err != nil {
		t.Fatal(err)
	}

	err = rsmq.ChangeMessageVisibility(qname, id, UnsetVt)

	if err == nil {
		t.Error("error is nil when changing visibility of non-existing message")
	}
	if err != ErrMessageNotFound {
		t.Error("error is not as expected")
	}

	err = rsmq.ChangeMessageVisibility("non-existing", id, UnsetVt)

	if err == nil {
		t.Error("error is nil when changing visibility of message in non-existing queue")
	}
	if err != ErrQueueNotFound {
		t.Error("error is not as expected")
	}

}

func TestRedisSMQ_DeleteMessage(t *testing.T) {

	client := preIntegrationTest(t)

	rsmq := NewRedisSMQ(client, "test")

	qname := "que"

	err := rsmq.CreateQueue(qname, UnsetVt, UnsetDelay, UnsetMaxsize)

	if err != nil {
		t.Fatal(err)
	}

	message := "message"

	id, err := rsmq.SendMessage(qname, message, UnsetVt)

	if err != nil {
		t.Fatal(err)
	}

	err = rsmq.DeleteMessage(qname, id)

	if err != nil {
		t.Fatal(err)
	}

	queMsg, err := rsmq.ReceiveMessage(qname, UnsetVt)

	if err != nil || queMsg != nil {
		t.Error("message is not deleted")
	}

	err = rsmq.DeleteMessage(qname, id)

	if err == nil {
		t.Error("error is nil when deleting message by non-existing message id")
	}
	if err != ErrMessageNotFound {
		t.Error("error is not as expected")
	}

	err = rsmq.DeleteMessage("non-existing", id)

	if err == nil {
		t.Error("error is nil when deleting message from non-existing queue")
	}
	if err != ErrQueueNotFound {
		t.Error("error is not as expected")
	}

}
