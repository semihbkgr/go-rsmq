package rsmq

import "testing"

func TestNewDefaultRedisSMQConfig(t *testing.T) {
	config := NewDefaultRedisSMQConfig()
	if config == nil {
		t.FailNow()
	}
}

func TestNewDefaultRedisSMQ(t *testing.T) {
	rsmq, err := NewDefaultRedisSMQ()
	if err != nil {
		t.Fatal(err.Error())
	}
	if rsmq == nil {
		t.Fatal("rsmq cannot be nil")
	}
	if rsmq.pool == nil {
		t.Fatal("rsmq.pool cannot be nil")
	}
	if rsmq.config == nil {
		t.Fatal("rsmq.config cannot be nil")
	}
	if rsmq.popMessageSha1 == "" {
		t.Fatal("rsmq.popMessageSha1 cannot be empty")
	}
	if rsmq.receiveMessageSha1 == "" {
		t.Fatal("rsmq.receiveMessageSha1 cannot be empty")
	}
	if rsmq.changeMessageVisibilitySha1 == "" {
		t.Fatal("rsmq.changeMessageVisibilitySha1 cannot be empty")
	}
}

func TestNewRedisSMQ(t *testing.T) {
	config := RedisSMQConfig{
		host: "localhost",
		port: 6379,
	}
	rsmq, err := NewRedisSMQ(&config)
	if err != nil {
		t.Fatal(err.Error())
	}
	if rsmq == nil {
		t.Fatal("rsmq cannot be nil")
	}
	if rsmq.pool == nil {
		t.Fatal("rsmq.pool cannot be nil")
	}
	if rsmq.config == nil {
		t.Fatal("rsmq.config cannot be nil")
	}
	if rsmq.popMessageSha1 == "" {
		t.Fatal("rsmq.popMessageSha1 cannot be empty")
	}
	if rsmq.receiveMessageSha1 == "" {
		t.Fatal("rsmq.receiveMessageSha1 cannot be empty")
	}
	if rsmq.changeMessageVisibilitySha1 == "" {
		t.Fatal("rsmq.changeMessageVisibilitySha1 cannot be empty")
	}
}
