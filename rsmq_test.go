package rsmq

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
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

func TestNewRedisSMQ(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

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

}

func TestRedisSMQ_CreateQueue(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	err = rsmq.CreateQueue("queue", 300, 100, 1024)

	if err != nil {
		t.Fatal(err)
	}

}

func TestRedisSMQ_ListQueues(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 300, 100, 1024)

	queues, err := rsmq.ListQueues()

	if err != nil {
		t.Fatal(err)
	}

	if len(queues) != 1 {
		t.Error("length of queues is not as expected")
	}

	if queues[0] != qname {
		t.Error("queue name is not as expected")
	}

}

func TestRedisSMQ_GetQueueAttributes(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 300, 100, 1024)

	queAttrib, err := rsmq.GetQueueAttributes(qname)

	if err != nil {
		t.Fatal(err)
	}

	if queAttrib == nil {
		t.Fatal("queue attributes is nil")
	}

}

func TestRedisSMQ_SetQueueAttributes(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 300, 100, 1024)

	queAttrib, err := rsmq.SetQueueAttributes(qname, 600, 200, 2048)

	if err != nil {
		t.Fatal(err)
	}

	if queAttrib == nil {
		t.Fatal("queue attributes is nil")
	}

}

func TestRedisSMQ_DeleteQueue(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 300, 100, 1024)

	err = rsmq.DeleteQueue(qname)

	if err != nil {
		t.Fatal(err)
	}

}

func TestRedisSMQ_SendMessage(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 300, 100, 1024)

	message := "message"

	_, err = rsmq.SendMessage(qname, message, 300)

	if err != nil {
		t.Fatal(err)
	}

}

func TestRedisSMQ_ReceiveMessage(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 300, 0, 1024)

	message := "message"

	id, _ := rsmq.SendMessage(qname, message, 0)

	queMsg, err := rsmq.ReceiveMessage(qname, 0)

	if err != nil {
		t.Fatal(err)
	}

	if queMsg == nil {
		t.Fatal("queue message is nil")
	}
	if queMsg.id != id {
		t.Error("id is not as expected")
	}
	if queMsg.message != message {
		t.Error("message is not as expected")
	}

}

func TestRedisSMQ_PopMessage(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 300, 0, 1024)

	message := "message"

	id, _ := rsmq.SendMessage(qname, message, 0)

	queMsg, err := rsmq.PopMessage(qname)

	if err != nil {
		t.Fatal(err)
	}

	if queMsg == nil {
		t.Fatal("queue message is nil")
	}
	if queMsg.id != id {
		t.Error("id is not as expected")
	}
	if queMsg.message != message {
		t.Error("message is not as expected")
	}

}

func TestRedisSMQ_ChangeMessageVisibility(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 300, 0, 1024)

	message := "message"

	id, _ := rsmq.SendMessage(qname, message, 0)

	err = rsmq.ChangeMessageVisibility(qname, id, 90)

	if err != nil {
		t.Fatal(err)
	}

}

func TestRedisSMQ_DeleteMessage(t *testing.T) {

	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	rc, err := setupRedis(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer rc.Container.Terminate(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: rc.address,
	})

	rsmq := NewRedisSMQ(client, "test")

	qname := "queue"

	rsmq.CreateQueue(qname, 0, 0, 1024)

	message := "message"

	id, _ := rsmq.SendMessage(qname, message, 0)

	err = rsmq.DeleteMessage(qname, id)

	if err != nil {
		t.Fatal(err)
	}

}
