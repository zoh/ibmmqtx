package main

import (
	"context"
	"github.com/sirupsen/logrus"
	ibmmqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
)

func workReq(ctx context.Context) (done chan struct{}) {
	done = make(chan struct{})
	go func() {
		cfg := config.GetMqConfig(logrus.TraceLevel)
		cfg.PutQueueName = Queue2
		cfg.GetQueueName = Queue1

		c, err := ibmmqtx.Dial(ctx, cfg)
		if err != nil {
			panic(err)
		}

		msg := &ibmmqtx.Msg{
			CorrelId: correlId,
			Payload:  []byte("test"),
		}
		msgNew, err := c.Request(ctx, msg, Queue2, Queue1)
		if err != nil {
			panic(err)
		}

		_ = msgNew.Commit()

		msgNew.PrintMsg()

		done <- struct{}{}
	}()
	return
}
