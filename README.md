# go-rsmq

### Redis Simple Message Queue

A lightweight message queue for Go that requires no dedicated queue server. Just a Redis server.

Go implementation of https://github.com/smrchy/rsmq.

### Example

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

```
queue exists
message sent, id: g92v9q70wmonrHFkSuohOf8IvDIjS5HU
message received, id: g92v9q70wmonrHFkSuohOf8IvDIjS5HU, message: message
```
