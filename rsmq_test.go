package rsmq

import (
	"context"
	"github.com/go-redis/redis"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"testing"
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
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	rsmq := NewRedisSMQ(client, "test-que")
	if rsmq == nil {
		t.Fatal("rsmq cannot be nil")
	}
	if rsmq.client == nil {
		t.Error("rsmq.pool cannot be nil")
	}
	if rsmq.namespace == "" {
		t.Error("rsmq.config cannot be nil")
	}
}
