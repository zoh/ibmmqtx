package main

import (
	"context"
	"github.com/sirupsen/logrus"
	ibmmqtx "github.com/zoh/ibmmqtx"
	"github.com/zoh/ibmmqtx/_examples/config"
	"log"
	"time"
)

func main() {
	cfg := config.GetMqConfig(logrus.TraceLevel)
	cfg.AutoReconnect = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mq, err := ibmmqtx.Dial(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}


	for {
		ctx, _ := context.WithTimeout(ctx, time.Second*10)
		msg, ok, err := mq.GetMessage(ctx)
		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			log.Fatal("not found messages.")
		}
		_ = msg.Commit()

		msg.PrintMsg()

		time.Sleep(time.Millisecond)
	}


	log.Println("end.")
}
