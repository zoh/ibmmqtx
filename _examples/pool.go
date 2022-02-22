package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	ibmmqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.GetMqConfig(logrus.TraceLevel)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-signals
		cancel()
	}()

	pool, err := ibmmqtx.CreatePool(ctx, cfg, 2, 10)
	if err != nil {
		panic(err)
	}

	var cons = make(chan *ibmmqtx.MQPro, 12)

	done := make(chan struct{})
	go func() {
		for {
			select {
			case c, ok := <-cons:
				if !ok {
					done <- struct{}{}
					return
				}

				err := pool.Put(c)
				if err != nil {
					panic(err)
				}
				time.Sleep(time.Second)
			}
		}
	}()

	go func() {
		for i := 0; i < 15; i++ {
			c, err := pool.Get()
			if err != nil {
				panic(err)
			}
			cons <- c
			logrus.Warn("coon # ", i)
		}
		//
		close(cons)
	}()

	<-done
	fmt.Printf("в пуле %+v \n", pool.Size())

	_ = pool.Flush()

	fmt.Printf("в пуле %+v \n", pool.Size())
}
