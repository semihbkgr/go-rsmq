[![Go Reference](https://pkg.go.dev/badge/github.com/semihbkgr/go-rsmq.svg)](https://pkg.go.dev/github.com/semihbkgr/go-rsmq)
![ci workflow](https://github.com/semihbkgr/go-rsmq/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/SemihBKGR/go-rsmq/branch/master/graph/badge.svg?token=IVOQ6PLNHM)](https://codecov.io/gh/SemihBKGR/go-rsmq)

# go-rsmq

A lightweight message queue for Go that requires no dedicated queue server. Just a Redis server.

Go implementation of https://github.com/smrchy/rsmq.

## Redis Simple Message Queue

If you run a Redis server and currently use Amazon SQS or a similar message queue you might as well use this fast little
replacement. Using a shared Redis server multiple Go processes can send / receive messages.

## Example

``` go

opts := &redis.Options{Addr: "localhost:6379"}
redisClient := redis.NewClient(opts)

rsmqClient := rsmq.NewRedisSMQ(redisClient, "rsmq")
defer rsmqClient.Quit()

err := rsmqClient.CreateQueue("queue", rsmq.UnsetVt, rsmq.UnsetDelay, rsmq.UnsetMaxsize)
if err != nil {
    fmt.Println(err.Error())
}

id, err := rsmqClient.SendMessage("queue", "message", rsmq.UnsetVt)
if err != nil {
    panic(err)
}
fmt.Printf("message sent, id: %s\n", id)

msg, err := rsmqClient.PopMessage("queue")
if err != nil {
    panic(err)
}
if msg == nil {
    fmt.Println("queue is empty")
} else {
    fmt.Printf("message received, id: %s, message: %s", msg.ID, msg.Message)
}

```

[Producer/Consumer example](./example_test.go)

## Implementation Notes

All details about the queue are [here](https://github.com/smrchy/rsmq/blob/master/README.md).

It follows all naming conventions of javascript implementation.
