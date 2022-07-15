package rsmq_test

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/semihbkgr/go-rsmq"
	"strconv"
	"time"
)

func Example_producer_consumer() {

	ns := "example"
	qname := "queue"

	prodTicker := time.NewTicker(time.Second * 3)
	prodClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	prodRsmq := rsmq.NewRedisSMQ(prodClient, ns)

	err := prodRsmq.CreateQueue(qname, rsmq.UnsetVt, rsmq.UnsetDelay, rsmq.UnsetMaxsize)
	if err != nil {
		fmt.Println(err.Error())
	}

	consTicker := time.NewTicker(time.Second * 1)
	consClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	consRsmq := rsmq.NewRedisSMQ(consClient, ns)

	for {
		select {
		case <-prodTicker.C:
			id, err := prodRsmq.SendMessage(qname, strconv.FormatInt(time.Now().UnixNano(), 10), rsmq.UnsetVt)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("message sent, id: %s\n", id)
			}
		case <-consTicker.C:
			msg, err := consRsmq.PopMessage(qname)
			if err != nil {
				fmt.Println(err)
			} else if msg != nil {
				fmt.Printf("message received, id: %s, message: %s\n", msg.ID, msg.Message)
			} else {
				fmt.Println("queue is empty")
			}
		}
	}

}
