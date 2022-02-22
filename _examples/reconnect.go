package main

import (
	"context"
	"github.com/sirupsen/logrus"
	mqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)
	cfg := config.GetMqConfig(logrus.TraceLevel)
	cfg.AutoReconnect = true

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	l := logrus.New()
	l.SetLevel(logrus.TraceLevel)

	mq, err := mqtx.Dial(ctx, cfg)
	if err != nil {
		logrus.Fatal(err)
	}

	var wg sync.WaitGroup

	// запускаем простой пинг
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msgID, tx, err := mq.PutMessage(&mqtx.Msg{
					Payload: []byte("test"),
				})
				if err != nil {
					logrus.Fatalln("ERRRR: ", err)
					//time.Sleep(time.Second)
					//continue
				}
				_ = tx.Commit()
				time.Sleep(time.Second * 3)
				logrus.Debugf("Отправили:!!!!!!!!! %x", msgID)
			}
		}
	}()

	go func() {
		<-signals
		cancel()
	}()

	wg.Wait()
	l.Info("Закончена отработка")
}
