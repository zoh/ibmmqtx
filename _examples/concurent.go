package main

import (
	"bytes"
	"context"
	"github.com/sirupsen/logrus"
	mqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

/**
  check concurrency mq-pool
  Request-Response
*/

var workCompleted uint32

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	l := logrus.New()
	l.SetLevel(logrus.TraceLevel)

	cfg := config.GetMqConfig(logrus.TraceLevel)
	pool, err := mqtx.CreatePool(ctx, cfg, 8, 100)
	if err != nil {
		logrus.Fatal(err)
	}

	var wg sync.WaitGroup

	// 100 запросов
	tasks := make(chan int, 50)

	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			tasks <- i
			time.Sleep(time.Millisecond * 10)
		}
		wg.Done()
	}()

	go func() {
		for i := range tasks {
			wg.Add(1)
			go work(pool, i, &wg)
		}
	}()

	go func() {
		<-signals
		cancel()
	}()

	wg.Wait()
	l.Info("Законцена отработка")

	l.Infof("Выполнено %d задач", workCompleted)
}

func work(pool mqtx.MQProPool, val int, wg *sync.WaitGroup) {
	defer wg.Done()

	client, err := pool.Get()
	if err != nil {
		panic(err)
	}

	logrus.Infof("Обрабатываем задачу %d", val)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*15)

	res, err := client.Request(ctx, &mqtx.Msg{Payload: []byte(strconv.Itoa(val))},
		"DEV.QUEUE.2", "DEV.QUEUE.1")
	if err != nil {
		logrus.Fatal(err, val)
	}

	_ = res.Commit()
	res.PrintMsg()

	if !bytes.Equal([]byte("Payload Response message: "+strconv.Itoa(val)), res.Payload) {
		panic("не сооствествие ожидаемому ответу")
	}

	err = pool.Put(client)
	if err != nil {
		panic(err)
	}

	atomic.AddUint32(&workCompleted, 1)
}
