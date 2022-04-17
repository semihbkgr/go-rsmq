package rsmq

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
	"time"
)

type redisContainer struct {
	testcontainers.Container
	host string
	port int
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

	return &redisContainer{Container: container, host: hostIP, port: mappedPort.Int()}, nil
}

func TestNewDefaultRedisSMQConfig(t *testing.T) {
	config := NewDefaultRedisSMQConfig()
	if config == nil {
		t.Fatal("config is nil")
	}
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

	config := RedisSMQConfig{
		host:    rc.host,
		port:    rc.port,
		timeout: 5 * time.Second,
	}
	rsmq, err := NewRedisSMQ(&config)
	if err != nil {
		t.Fatal(err.Error())
	}
	if rsmq == nil {
		t.Fatal("rsmq cannot be nil")
	}
	if rsmq.pool == nil {
		t.Error("rsmq.pool cannot be nil")
	}
	if rsmq.config == nil {
		t.Error("rsmq.config cannot be nil")
	}
	if rsmq.popMessageSha1 == "" {
		t.Error("rsmq.popMessageSha1 cannot be empty")
	}
	if rsmq.receiveMessageSha1 == "" {
		t.Error("rsmq.receiveMessageSha1 cannot be empty")
	}
	if rsmq.changeMessageVisibilitySha1 == "" {
		t.Error("rsmq.changeMessageVisibilitySha1 cannot be empty")
	}
}
