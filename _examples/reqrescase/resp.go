package main

import (
	"context"
	"github.com/sirupsen/logrus"
	ibmmqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"log"
)

func workResp(ctx context.Context) {
	cfg := config.GetMqConfig(logrus.TraceLevel)
	cfg.PutQueueName = Queue1
	cfg.GetQueueName = Queue2

	c, err := ibmmqtx.Dial(ctx, cfg)
	if err != nil {
		panic(err)
	}
	defer c.Disconnect()

	// достаём из Queue1 перекладываем в

	for {
		msg, ok, err := c.GetMessageByCorrelID(ctx, correlId)
		if err != nil {
			panic(err)
		}
		if ok == false {
			log.Println("no messages")
			return
		}

		log.Println("get msg")
		msg.PrintMsg()
		_ = msg.Commit()

		go sendResp(c, msg.Msg)
	}
}

func sendResp(c *ibmmqtx.MQPro, msg *ibmmqtx.Msg) {
	_, tx, err := c.PutMessage(&ibmmqtx.Msg{
		CorrelId: msg.CorrelId,
		Payload:  []byte("send resp: " + string(msg.Payload)),
	})
	if err != nil {
		panic(err)
	}
	_ = tx.Commit()
}
