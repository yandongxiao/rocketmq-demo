package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	sig := make(chan os.Signal)
	c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{"127.0.0.1:9876"}),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithConsumeMessageBatchMaxSize(1),
		// consumer.WithPullInterval(10*time.Second),
		// consumer.WithPullBatchSize(1000),
		// consumer.WithConsumeGoroutineNums(5),
		consumer.WithGroupName("GID_ZZZ"),
		consumer.WithMaxReconsumeTimes(1),
		consumer.WithConsumeTimeout(3600*time.Second),
	)
	err = c.Subscribe("test2", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		concurrentCtx, _ := primitive.GetConcurrentlyCtx(ctx)
		concurrentCtx.DelayLevelWhenNextConsume = 1
		// for i := range msgs {
		// 	fmt.Printf("time: %v, subscribe callback: %v \n", time.Now(), string(msgs[i].Body))
		// }
		fmt.Println("begin: len(msgs)", len(msgs))
		time.Sleep(time.Second * 3000)
		fmt.Println("end: len(msgs)", len(msgs))

		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	// Note: start after subscribe
	err = c.Start()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	<-sig
	err = c.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s", err.Error())
	}
}
